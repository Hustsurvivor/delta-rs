//! Run full, filtered, and time-travel scans over the prepared lineitem table.
//! Metadata is timed during plan construction; the data scan runs afterward.

use std::fs;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::Parser;
use deltalake_core::datafusion::logical_expr::Expr;
use deltalake_core::datafusion::prelude::{col, lit, SessionContext};
use deltalake_core::datafusion::catalog::TableProvider;
use deltalake_core::delta_datafusion::TableProviderBuilder;
use deltalake_core::metadata_metrics::MetadataMetrics;
use deltalake_core::{ensure_table_uri, DeltaTable};
use futures::TryStreamExt;
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(about = "Run lineitem full, filtered, and time-travel scans")]
struct Cli {
    /// Prepared Delta table created by write_lineitem.
    #[arg(long, default_value = "damlake_test/output/lineitem_5rows_delta")]
    table: PathBuf,
    /// Number of repetitions for each scan scenario.
    #[arg(long, default_value_t = 3)]
    runs: usize,
    /// Historical version. Defaults to the previous version.
    #[arg(long)]
    history_version: Option<u64>,
    /// CSV destination. A JSON array is written alongside it.
    #[arg(long, default_value = "damlake_test/output/lineitem_scan_metrics.csv")]
    csv: PathBuf,
}

#[derive(Serialize)]
struct Row {
    run_id: String,
    operation: &'static str,
    stage: String,
    duration_ns: u128,
    table_path: String,
    target_version: u64,
    latest_version: u64,
    active_files: usize,
    candidate_files: usize,
    rows_read: usize,
    log_files: Option<u64>,
    pruning_ratio: f64,
    aggregation: &'static str,
}

fn append_rows(
    rows: &mut Vec<Row>,
    metrics: &MetadataMetrics,
    operation: &'static str,
    table: String,
    target: u64,
    latest: u64,
    active: usize,
    candidates: usize,
    rows_read: usize,
) {
    let pruning_ratio = if active == 0 { 0.0 } else { 1.0 - candidates as f64 / active as f64 };
    let stages = metrics.stages();
    let total: u128 = stages.iter().filter(|s| matches!(s.name,
        "snapshot_reconstruction" | "predicate_preparation" |
        "metadata_file_pruning" | "active_file_enumeration"
    )).map(|s| s.elapsed.as_nanos()).sum();
    for (name, duration) in stages.iter().map(|s| (s.name, s.elapsed.as_nanos()))
        .chain(std::iter::once(("query_metadata_total", total))) {
        rows.push(Row {
            run_id: metrics.run_id().to_owned(),
            operation,
            stage: name.to_owned(),
            duration_ns: duration,
            table_path: table.clone(),
            target_version: target,
            latest_version: latest,
            active_files: active,
            candidate_files: candidates,
            rows_read,
            log_files: None,
            pruning_ratio,
            aggregation: if name == "query_metadata_total" { "sum_non_overlapping" } else { "inclusive_stage" },
        });
    }
}

async fn run_scan(
    table_uri: url::Url,
    version: Option<u64>,
    filters: Vec<Expr>,
    metrics: MetadataMetrics,
) -> Result<(u64, usize, usize)> {
    let session = SessionContext::new();
    let table = DeltaTable::try_from_url(table_uri).await?;
    let provider = TableProviderBuilder::default()
        .with_log_store(table.log_store())
        .with_table_version(version)
        .with_metadata_metrics(metrics.clone())
        .build().await?;
    let actual_version = provider.snapshot_version() as u64;
    let plan = provider.scan(&session.state(), None, &filters, None).await?;
    let candidates = metrics.candidate_files().context("scan did not record candidate files")?;

    // Execute and drain the actual scan after metadata timing has stopped.
    let mut rows_read = 0usize;
    for partition in 0..plan.properties().output_partitioning().partition_count() {
        let mut stream = plan.execute(partition, session.task_ctx())?;
        while let Some(batch) = stream.try_next().await? {
            rows_read += batch.num_rows();
        }
    }
    Ok((actual_version, candidates, rows_read))
}

async fn scan_lineitem(cli: Cli) -> Result<()> {
    let table_uri = ensure_table_uri(cli.table.to_string_lossy().as_ref())?;
    let invocation = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let mut rows = Vec::new();

    for sample in 0..cli.runs {
        // Full scan at the latest version.
        let metrics = MetadataMetrics::new(format!("{invocation}-full-{sample}"));
        let (latest, active, full_rows) = run_scan(table_uri.clone(), None, vec![], metrics.clone()).await?;
        append_rows(&mut rows, &metrics, "full_scan", table_uri.to_string(), latest, latest, active, active, full_rows);

        let historical = cli.history_version.unwrap_or_else(|| latest.saturating_sub(1));
        if historical > latest {
            bail!("history version {historical} is outside 0..={latest}");
        }

        // Time travel through the same table's real transaction history.
        let metrics = MetadataMetrics::new(format!("{invocation}-time-travel-{sample}"));
        let (history_actual, historical_files, historical_rows) = run_scan(table_uri.clone(), Some(historical), vec![], metrics.clone()).await?;
        append_rows(&mut rows, &metrics, "time_travel", table_uri.to_string(), history_actual, latest, historical_files, historical_files, historical_rows);

        // Partition pruning scan.
        let metrics = MetadataMetrics::new(format!("{invocation}-partition-filter-{sample}"));
        let (actual, candidates, scanned_rows) = run_scan(table_uri.clone(), Some(latest), vec![col("shipmode").eq(lit("AIR"))], metrics.clone()).await?;
        append_rows(&mut rows, &metrics, "filter_scan_partition", table_uri.to_string(), actual, latest, active, candidates, scanned_rows);

        // Statistics pruning scan on a non-partition column.
        let metrics = MetadataMetrics::new(format!("{invocation}-statistics-filter-{sample}"));
        let (actual, candidates, scanned_rows) = run_scan(table_uri.clone(), Some(latest), vec![col("quantity").gt(lit(30.0))], metrics.clone()).await?;
        append_rows(&mut rows, &metrics, "filter_scan_statistics", table_uri.to_string(), actual, latest, active, candidates, scanned_rows);
    }

    if let Some(parent) = cli.csv.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut csv = String::from("run_id,operation,stage,duration_ns,table_path,target_version,latest_version,active_files,candidate_files,rows_read,log_files,pruning_ratio,aggregation\n");
    for row in &rows {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            row.run_id, row.operation, row.stage, row.duration_ns, row.table_path,
            row.target_version, row.latest_version, row.active_files,
            row.candidate_files, row.rows_read, row.log_files.map(|n| n.to_string()).unwrap_or_default(), row.pruning_ratio, row.aggregation
        ));
    }
    fs::write(&cli.csv, csv)?;
    fs::write(cli.csv.with_extension("json"), serde_json::to_vec_pretty(&rows)?)?;
    println!("wrote {} scan metadata records to {}", rows.len(), cli.csv.display());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    scan_lineitem(Cli::parse()).await
}
