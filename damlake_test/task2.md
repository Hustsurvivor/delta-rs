**结论：当前埋点还不能准确表示 Delta Lake 正常读写路径中“元数据处理部分”的时延。** 读侧接入了真实的 DataFusion 扫描调用链，但计时边界偏宽；写侧尚未接入计时。现有改动没有明显改写正常读写逻辑，但不能据此认为测量结果已经满足目标。

主要发现：

1. **写入元数据时延没有被记录。** [write_lineitem.rs](D:/project/aicoding/delta-rs/damlake_test/write_lineitem.rs:119) 执行了 `CommitBuilder::build` 和随后的 `table.load()`，却没有使用 `MetadataMetrics`。因此无法得到提交准备、事务日志写入、冲突检查或提交后快照刷新的耗时。

2. **读侧“文件裁剪”阶段计入了其他工作。** [next/mod.rs](D:/project/aicoding/delta-rs/crates/core/src/delta_datafusion/table_provider/next/mod.rs:779) 的计时包住整个 `execution_plan()`；该函数在回放和筛选文件后还构造 DataFusion/Parquet 物理计划，带 deletion vector 时甚至会读取数据文件的 Parquet footer（[scan/mod.rs](D:/project/aicoding/delta-rs/crates/core/src/delta_datafusion/table_provider/next/scan/mod.rs:665)）。所以 `metadata_file_pruning` 和 `active_file_enumeration` 不是纯粹的 Delta 日志元数据处理时间。`query_scan_planning` 又包住上述阶段，两者是包含关系，不能相加。

3. **辅助加载方法与正常 `load()` 路径不等价。** [load_with_metadata_metrics](D:/project/aicoding/delta-rs/crates/core/src/table/mod.rs:201) 先单独解析最新版本，再调用 `load_version(latest)`；正常 `load()` 直接调用 `update_incremental(None)`。这增加一次版本查询，并把目标版本固定在两次调用之间。若用它代表正常加载的延迟，结果会有偏差。目前基准程序也没有调用这个辅助方法。

4. **候选文件数与计时的扫描并非同一次操作。** [scan_lineitem.rs](D:/project/aicoding/delta-rs/damlake_test/scan_lineitem.rs:145) 在扫描结束后，通过另一个表句柄和另一套谓词单独统计候选文件。这个数字不能直接证明被计时扫描实际裁剪了相同数量的文件。

建议把计时点放在现有读写调用内部的窄边界：读侧分别围住快照构建、元数据流回放与文件选择；写侧围住提交准备、提交执行及必要的快照刷新。保持现有调用顺序，并从**同一次扫描**取得候选文件数。这样才能同时验证时延归属和流程未改变。