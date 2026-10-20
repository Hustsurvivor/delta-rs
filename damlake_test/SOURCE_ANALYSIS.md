# Delta Lake 元数据实验：源码映射与埋点方案

本说明基于当前工作区实际检出的 `deltalake-core` 源码编写，不依赖对上游目录结构的预设。

| 阶段 | 实际源码位置与调用链 | I/O / 求值边界 | 计量方式 |
|---|---|---|---|
| 最新或历史版本解析 | `TableProviderBuilder::build` → `Snapshot::try_new` | 构建指定版本或最新版本快照；不访问数据文件 | `snapshot_reconstruction` |
| 快照发现、Checkpoint 查找、JSON 日志解析 | `kernel/snapshot/mod.rs`：`Snapshot::try_new` → `try_new_with_engine_and_catalog` → `KernelSnapshot::builder_for(...).build(...)` | 当前 kernel builder 将发现、`_last_checkpoint` 查找、Checkpoint/日志读取与 Action 解码作为一个同步构建过程处理 | 纳入 `snapshot_reconstruction`；除非修改 kernel API，否则不能在不改变语义的前提下再细分 |
| Checkpoint 提示刷新 | `kernel/snapshot/mod.rs`：`read_last_checkpoint_version` | 读取 `_delta_log/_last_checkpoint`；不访问数据文件 | 有独立源码边界，但它不属于当前 kernel 对新快照构建所公开的主路径 |
| 活跃 Add 状态物化 | `EagerSnapshot::try_new_with_snapshot` → `Snapshot::ensure_materialized_files` → `materialize_files_with_engine` | 将日志/Checkpoint 元数据回放至 Arrow batches；这是纯元数据操作，不打开用户数据 Parquet 文件 | 纳入 `snapshot_reconstruction` |
| 谓词准备 | `KernelScanPlan::try_new_with_contract_impl` → `process_filters` | 只转换分区和统计裁剪谓词 | `predicate_preparation` |
| 活跃文件枚举或裁剪 | `scan::execution_plan` → `replay_files` | 消费同一次日志元数据流并选出候选文件；不包括后续 Parquet 计划构造 | `active_file_enumeration` 或 `metadata_file_pruning` |
| 提交 | `CommitBuilder` → `PreCommit` → `PreparedCommit` → `PostCommit` | 提交载荷准备、验证、实际冲突检测、各次原子日志写入及提交后处理 | 分别记录对应阶段；未进入冲突分支不生成该阶段 |
| Checkpoint 创建 | `protocol/checkpoints.rs`：`create_checkpoint_for` | 写入 Delta Checkpoint 元数据，而不是表数据 | 显式调用时可单独计量 |

## 已实现的埋点

`deltalake-core` 新增可选 Cargo feature：`metadata-metrics`，以及位于
`crates/core/src/metadata_metrics.rs` 的显式、单次运行级别
`MetadataMetrics` 收集器。该收集器使用 `Instant`，不使用全局计时器；每轮实验由调用方提供唯一 `run_id`。

`TableProviderBuilder::with_metadata_metrics` 将同一收集器传递给真实的
DataFusion 查询入口。埋点位于下列实际调用链，而不是基准程序外围的手工包装：

`TableProviderBuilder::build` → `Snapshot::try_new` → `DeltaScan::scan` →
`scan::execution_plan` → 元数据流回放与候选文件选择。

其中记录：

- `snapshot_reconstruction`：在 provider 构建时创建目标版本快照；
- `predicate_preparation`：转换过滤谓词；
- `active_file_enumeration`：无过滤条件时回放活跃文件元数据；
- `metadata_file_pruning`：带分区或统计信息过滤条件时完成候选文件选择；
- `query_metadata_total`：对上述互不重叠的元数据阶段求和，不含 DataFusion/Parquet 物理计划构造。

单项阶段均为包含式墙钟时间；`query_metadata_total` 是所列互不重叠阶段之和，不能再与其子阶段相加。当前 kernel 将发现、I/O、JSON/Parquet Action 解码、Add/Remove 回放和状态构建暴露为一个原子 builder 操作。无法从本次操作取得的日志文件数输出为空值，不能按版本号推算。

写侧记录 `commit_payload_build`、`commit_payload_preparation`、每次
`latest_version_validation`、实际触发时的 `conflict_detection`、每次
`atomic_log_write`、`post_commit_snapshot_processing`，以及驱动程序随后调用
`table.load()` 的 `table_refresh`。每个阶段的 `inclusive` 字段为 `true`；重试时同名阶段可重复出现，不能把这些阶段与其他包含式父阶段重复相加。Parquet 数据文件写入发生在提交收集器创建之前，不纳入这些阶段。

## 可复现的 lineitem 工作负载

`damlake_test/write_lineitem.rs` 在建表阶段解析给定的
管道分隔 TPC-H `lineitem.tbl` 文件。它保留：

- `orderkey`
- `quantity`
- `shipdate`
- `shipmode`

`write_lineitem.rs` 将数据写入按 `shipmode` 分区的 Delta 表：先按分区分组，再把每五行写成一个 Parquet 文件，并每累积五个文件（五个 Add actions）提交一次 Delta 事务；最后不足五个文件时提交一个短事务。默认 fixture 路径为 `damlake_test/output/lineitem_5rows_delta`。

`damlake_test/data/` 与 `damlake_test/output/` 不进入 Git；新克隆仓库需自行将 `lineitem.tbl` 放入默认输入路径，或通过 `write_lineitem --input` 指定数据文件。

`scan_lineitem.rs` 会实际调用 DataFusion `TableProvider::scan`，使 DeltaScan 在构建物理计划时回放元数据流；随后执行并耗尽物理计划，以完成真实的数据扫描。元数据计时器在调用 `ExecutionPlan::execute` 之前关闭，因此数据 Parquet 的读取、解码与行级计算不会计入元数据时延。下列计时实验均分别记录元数据阶段：

- `full_scan`：读取最新版本并扫描全部活跃文件；
- `time_travel`：读取同一表日志历史中的前一提交版本并扫描；
- `filter_scan_partition`：执行 `shipmode = 'AIR'` 分区过滤扫描；
- `filter_scan_statistics`：执行 `quantity > 30` 的统计信息过滤扫描。

候选文件数从被计时的同一次 `replay_files` 获得。全量扫描数作为同版本过滤扫描的活跃文件基数，并将后续扫描固定到全量扫描实际使用的版本。数据扫描仍会执行，但数据文件的 Footer/Row Group 访问与记录解码不落入元数据阶段。写侧每个五文件提交有独立 `run_id`，提交后另计 `table_refresh`。两个程序分别输出 CSV 和 JSON。

通过 Docker 运行：

```powershell
& 'C:\Users\lgn\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe' run --rm -v "${PWD}:/workspace" -w /workspace rust:1.95 cargo run -p delta-benchmarks --bin write_lineitem

# Run scans after the write process has prepared the table.
& 'C:\Users\lgn\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe' run --rm -v "${PWD}:/workspace" -w /workspace rust:1.95 cargo run -p delta-benchmarks --bin scan_lineitem -- --runs 3
```

写入程序会拒绝覆盖非空 fixture 目录。若需重复运行写入，请给 `--table` 指定一个新的空目录。
