//! Write lineitem.tbl into a shipmode-partitioned Delta table.
//!
//! Each data file contains up to five rows from one shipmode partition. Every
//! five files are committed together; a final partial group forms one commit.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use deltalake_core::arrow::array::{Float64Array, Int64Array, StringArray};
use deltalake_core::arrow::datatypes::{DataType, Field, Schema};
use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::kernel::transaction::CommitBuilder;
use deltalake_core::metadata_metrics::MetadataMetrics;
use deltalake_core::kernel::{Action, Add, DataType as DeltaType, PrimitiveType, StructField};
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::writer::{DeltaWriter as _, RecordBatchWriter};
use deltalake_core::{ensure_table_uri, DeltaTable};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(about = "Write lineitem.tbl as a partitioned Delta table")]
struct Cli {
    /// Input TPC-H lineitem file.
    #[arg(long, default_value = "damlake_test/data/lineitem.tbl")]
    input: PathBuf,
    /// Destination Delta table; must be absent or empty.
    #[arg(long, default_value = "damlake_test/output/lineitem_5rows_delta")]
    table: PathBuf,
    /// CSV destination. A JSON array is written alongside it.
    #[arg(long, default_value = "damlake_test/output/lineitem_write_metrics.csv")]
    csv: PathBuf,
}

#[derive(Serialize)]
struct MetricRow {
    run_id: String,
    stage: String,
    duration_ns: u128,
    table_path: String,
    committed_version: u64,
    files_in_commit: usize,
    inclusive: bool,
}

#[derive(Debug)]
struct LineitemRow {
    orderkey: i64,
    quantity: f64,
    shipdate: String,
    shipmode: String,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("orderkey", DataType::Int64, false),
        Field::new("quantity", DataType::Float64, false),
        Field::new("shipdate", DataType::Utf8, false),
        Field::new("shipmode", DataType::Utf8, false),
    ]))
}

fn delta_schema() -> Vec<StructField> {
    vec![
        StructField::new("orderkey", DeltaType::Primitive(PrimitiveType::Long), false),
        StructField::new("quantity", DeltaType::Primitive(PrimitiveType::Double), false),
        StructField::new("shipdate", DeltaType::Primitive(PrimitiveType::String), false),
        StructField::new("shipmode", DeltaType::Primitive(PrimitiveType::String), false),
    ]
}

fn lineitem_batches(input: &PathBuf) -> Result<Vec<RecordBatch>> {
    let text = fs::read_to_string(input).with_context(|| format!("read {}", input.display()))?;
    let mut partitions: BTreeMap<String, Vec<LineitemRow>> = BTreeMap::new();
    for (line_no, line) in text.lines().enumerate() {
        let fields: Vec<_> = line.trim_end_matches('|').split('|').collect();
        if fields.len() < 15 {
            bail!("line {} has {} columns; expected TPC-H lineitem", line_no + 1, fields.len());
        }
        let shipmode = fields[14].to_owned();
        partitions.entry(shipmode.clone()).or_default().push(LineitemRow {
            orderkey: fields[0].parse::<i64>().with_context(|| format!("orderkey at line {}", line_no + 1))?,
            quantity: fields[4].parse::<f64>().with_context(|| format!("quantity at line {}", line_no + 1))?,
            shipdate: fields[10].to_owned(),
            shipmode,
        });
    }
    if partitions.is_empty() {
        bail!("input contains no rows");
    }

    // Keep each batch within one partition so a flush yields exactly one file.
    Ok(partitions.into_values().flat_map(|rows| {
        rows.chunks(5).map(|chunk| {
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from_iter_values(chunk.iter().map(|row| row.orderkey))),
                    Arc::new(Float64Array::from_iter_values(chunk.iter().map(|row| row.quantity))),
                    Arc::new(StringArray::from_iter_values(chunk.iter().map(|row| row.shipdate.as_str()))),
                    Arc::new(StringArray::from_iter_values(chunk.iter().map(|row| row.shipmode.as_str()))),
                ],
            )
            .expect("lineitem Arrow schema is valid")
        }).collect::<Vec<_>>()
    }).collect())
}

async fn commit_adds(table: &mut DeltaTable, adds: Vec<Add>, metrics: &MetadataMetrics) -> Result<u64> {
    let finalized = CommitBuilder::default()
        .with_metadata_metrics(metrics.clone())
        .with_actions(adds.into_iter().map(Action::Add).collect())
        .build(
            Some(table.snapshot()?.snapshot()),
            table.log_store(),
            DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: Some(vec!["shipmode".to_owned()]),
                predicate: None,
            },
        )
        .await?;
    {
        let _refresh = metrics.stage("table_refresh");
        table.load().await?;
    }
    Ok(finalized.version)
}

async fn write_lineitem(cli: Cli) -> Result<()> {
    if cli.table.exists() && fs::read_dir(&cli.table)?.next().is_some() {
        bail!("fixture directory {} is not empty; refusing to overwrite it", cli.table.display());
    }
    fs::create_dir_all(&cli.table)?;
    let url = ensure_table_uri(cli.table.to_string_lossy().as_ref())?;
    let mut table = DeltaTable::try_from_url(url.clone())
        .await?
        .create()
        .with_columns(delta_schema())
        .with_partition_columns(["shipmode"])
        .await?;

    let mut pending_adds = Vec::with_capacity(5);
    let mut metric_rows = Vec::new();
    let mut commit_number = 0usize;
    for batch in lineitem_batches(&cli.input)? {
        let mut writer = RecordBatchWriter::for_table(&table)?;
        writer.write(batch).await?;
        let adds = writer.flush().await?;
        if adds.len() != 1 {
            bail!("expected one data file per five-row batch, got {}", adds.len());
        }
        pending_adds.extend(adds);
        if pending_adds.len() == 5 {
            let files = std::mem::take(&mut pending_adds);
            commit_number += 1;
            record_commit(&mut table, files, commit_number, &url, &mut metric_rows).await?;
        }
    }
    if !pending_adds.is_empty() {
        commit_number += 1;
        record_commit(&mut table, pending_adds, commit_number, &url, &mut metric_rows).await?;
    }
    if let Some(parent) = cli.csv.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut csv = String::from("run_id,stage,duration_ns,table_path,committed_version,files_in_commit,inclusive\n");
    for row in &metric_rows {
        csv.push_str(&format!("{},{},{},{},{},{},{}\n", row.run_id, row.stage,
            row.duration_ns, row.table_path, row.committed_version, row.files_in_commit, row.inclusive));
    }
    fs::write(&cli.csv, csv)?;
    fs::write(cli.csv.with_extension("json"), serde_json::to_vec_pretty(&metric_rows)?)?;
    println!("wrote lineitem Delta table to {}", cli.table.display());
    Ok(())
}

async fn record_commit(
    table: &mut DeltaTable,
    adds: Vec<Add>,
    commit_number: usize,
    table_uri: &url::Url,
    rows: &mut Vec<MetricRow>,
) -> Result<()> {
    let files_in_commit = adds.len();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let metrics = MetadataMetrics::new(format!("write-{commit_number}-{nonce}"));
    let version = commit_adds(table, adds, &metrics).await?;
    for stage in metrics.stages() {
        rows.push(MetricRow {
            run_id: metrics.run_id().to_owned(),
            stage: stage.name.to_owned(),
            duration_ns: stage.elapsed.as_nanos(),
            table_path: table_uri.to_string(),
            committed_version: version,
            files_in_commit,
            inclusive: true,
        });
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    write_lineitem(Cli::parse()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stale_snapshot_records_only_real_conflict_checks() -> Result<()> {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema())
            .with_partition_columns(["shipmode"])
            .await?;
        let first = MetadataMetrics::new("first-writer");
        let first_commit = CommitBuilder::default()
            .with_metadata_metrics(first.clone())
            .build(Some(table.snapshot()?.snapshot()), table.log_store(), DeltaOperation::FileSystemCheck {});
        let second = MetadataMetrics::new("stale-writer");
        let second_commit = CommitBuilder::default()
            .with_metadata_metrics(second.clone())
            .build(Some(table.snapshot()?.snapshot()), table.log_store(), DeltaOperation::FileSystemCheck {});

        // Both transactions are prepared from version 0. The first advances the log;
        // the second must validate against that concurrent commit before writing.
        first_commit.await?;
        assert!(!first.stages().iter().any(|stage| stage.name == "conflict_detection"));
        second_commit.await?;
        assert!(second.stages().iter().any(|stage| stage.name == "conflict_detection"));
        Ok(())
    }
}
