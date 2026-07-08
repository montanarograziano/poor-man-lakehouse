---
title: DuckLake vs Delta-rs Benchmark — Assumptions and Limitations
date: 2026-04-26
status: draft
---

# DuckLake vs Delta-rs Benchmark — Assumptions and Limitations

This document captures methodology assumptions, known limitations, and disclosure points for the
benchmark in [notebooks/benchmark/](../../../notebooks/benchmark/). Companion to the implementation
plan at `~/.claude/plans/cozy-growing-kettle.md`.

The goal is to make every non-obvious choice and every known weakness explicit, so:
- conference-talk slides can disclose them honestly,
- future maintainers know what's load-bearing vs incidental,
- audience questions ("did you account for X?") have a documented answer.

## Headline goal of the benchmark

**Can we write 100 GB of synthetic tabular data on a 16 GB RAM machine, with both DuckLake v1.0
and Delta-rs, and read it back?**

Everything else (per-operation timings, storage efficiency, scan ratios) is secondary. The primary
deliverable is a streaming-correct pipeline that bounds memory by chunk size, not total dataset
size.

## Methodology assumptions

### Single-process, single-host

- One Python process, one machine, one storage backend at a time.
- No concurrency, no multi-writer contention, no distributed compute.
- Polars and DuckDB use their own internal threading (default settings), not constrained.
- Implication: results characterize *single-node engine performance*, not production-scale
  cluster behavior. Disclose explicitly.

### Writer memory is dominated by `target_file_size`, not `chunk_size`

- The streaming chunk size (`benchmark.batch_size`, default 100K rows) bounds **generator**
  memory: each `pa.RecordBatch` is ~20 MB at the default schema width. This is fine.
- The Parquet **writer** memory is a different beast. delta-rs and DuckDB both buffer per
  partition until they reach `target_file_size`, then flush a file. The default for delta-rs
  is large (~100 MB compressed → ~300-500 MB uncompressed in Arrow form per open partition).
  With even 1-2 active partitions in our sorted stream, the working set is hundreds of MB
  per active partition; with naive (random-event_date) input the working set scales as
  `partitions × target_file_size` and OOMs at scale.
- We mitigate **two ways**:
  1. **Sorted streams** (`StreamingGenerator` emits rows in `event_date` order via id-based
     partition assignment). The Parquet writer only ever has 1-2 partitions open
     simultaneously.
  2. **Explicit `target_file_size` of 16 MB** (`benchmark.target_file_size_mb`). Empirically
     measured: at 10M rows, Delta writer Δ RSS dropped from 1448 MB (default) to 133 MB
     (target=16MB). At 1M rows: 1449 MB → 116 MB with target=2MB. Trade-off is more files
     and slightly worse compression headroom (~7% larger total disk at 10M scale).
- Tunable via `benchmark.target_file_size_mb` in YAML. Smaller values bound writer memory
  more tightly at the cost of file count. For the 100 GB target, 16 MB gives ~1500 files
  per partition (~50K total) — still readable but worth disclosing.
- `parquet_row_group_size` is also tunable (default 122,880 rows). Smaller row groups give
  finer-grained predicate pushdown but more metadata overhead. Defaults are reasonable.
- DuckLake/DuckDB does not expose a per-`INSERT INTO` `target_file_size` knob the same way;
  we set `PRAGMA preserve_insertion_order = false` to let DuckDB pack files better, which
  combined with sorted input keeps memory bounded. If the talk needs to claim DuckLake file
  sizing parity, run with multiple `target_file_size` values on Delta only and disclose that
  DuckLake's file count is engine-determined.

### Partition cardinality is bounded to ~30

- Default `event_date` range is **30 days** (2024-01-01 to 2024-01-30), giving ~30 partitions
  regardless of total row count.
- Why bounded: at 100M rows over 30 partitions you get ~3.3M rows/partition (healthy Parquet
  files); at 10K rows over 30 partitions you get ~330 rows/partition (small but writable). At
  730 partitions (one year of daily granularity), 10K rows yields ~14 rows/partition — DuckDB
  opens 730 simultaneous Parquet writers, each with multi-MB write buffers, and blows past
  16 GB RAM during chained `merge_upsert` operations. We discovered this the hard way.
- Implication: the benchmark exercises partitioning as a *feature flag*, not as a stress test
  of high-cardinality partitioning. If the talk needs to demo "what happens with 365 daily
  partitions on small data", widen the date range explicitly via a custom `GeneratorSpec` and
  expect dramatically higher write memory.
- The filter `date_range` is set to roughly 1/3 of the partition span (`2024-01-10` to
  `2024-01-20`) so partition pruning is observable but not trivial.

### Synthetic data, not workload-realistic

- Schema is broad (18 columns covering integers, floats, decimals, temporal, varchar, text,
  boolean, list, struct, map) — exercises type coverage, not a specific real-world workload.
- ID column is sequential `int64`, partition column `event_date` is uniform random over a 5-year
  span. Real workloads have skew; we don't simulate it.
- Cardinality of `varchar_col` is 1000 (dict-encoded). Real low-cardinality columns are often
  much smaller (10s) or much larger (millions); both extremes change Parquet dict-page behavior.
- Numerical column ranges are wide (e.g. `int64` over the full 2^55 range). This stresses encoding
  but is unrepresentative of business data.

### Filter and aggregation predicates are fixed

- Filtered scan: `event_date BETWEEN '2023-01-01' AND '2023-12-31' AND varchar_col IN
  ('value_001','value_002','value_003')`.
- Aggregation: `GROUP BY varchar_col, agg(int64_col, float64_col, date_col)`.
- These exercise predicate pushdown and column pruning, but **only for the columns we picked**.
- Different choices (e.g. filtering by `text_col LIKE`, aggregating by struct field) would
  produce different rankings. Disclose that any one workload chosen is illustrative, not
  conclusive.

### One Delta version / one DuckLake snapshot per write call

- The streaming write path uses `pa.RecordBatchReader` consumed in a single `write_deltalake(...)`
  call (Delta) and a single `INSERT INTO ... SELECT * FROM <reader>` (DuckLake), so an entire
  write produces exactly one new Delta version / one new DuckLake snapshot regardless of chunk
  count.
- This is the fair comparison. The naive "loop and append per batch" approach (rejected for the
  default benchmark) would produce N versions, distorting both write time and on-disk size.

### Write-once / read-many for read benchmarks

- Read operations (`read_full_scan`, `read_filtered_scan`, `read_aggregation`) share a single
  pre-populated table per `(engine, storage_mode, size)` combo. Each read is measured `repeat_runs`
  times.
- Implication: page cache, OS-level caches, and DuckDB/Polars internal caches *are warm* on
  repeated reads. This is realistic for read-heavy workloads but biases against cold-start
  scenarios. We do one warmup run before timing to make this explicit (warm-cache numbers, not
  cold-cache).

### Median of N runs

- `warmup_runs=1, repeat_runs=3` by default. Median taken across timed runs.
- N=3 is small; outliers are possible. For the talk, configure `repeat_runs=5` and disclose the
  spread (min/max) rather than just the median if you have time.

## Memory metric: psutil RSS

- `peak_rss_mb` and `delta_rss_mb` are measured by sampling `psutil.Process().memory_info().rss`
  in a background thread at 50 ms intervals.
- `peak_rss_mb` is the absolute peak; `delta_rss_mb` is `peak − baseline_at_op_start`.
- **Why not tracemalloc?** It only sees Python heap allocations; DuckDB, delta-rs, and Polars
  allocate the bulk of their working set in C++/Rust outside Python. tracemalloc undercounts by
  10-100x for these workloads. It also slows execution 2-10x.
- **Why RSS, not USS?** RSS is what the OS reports as "resident memory" — what `htop` shows. It
  is what eats your 16 GB. USS (unique set size) is shared-page-aware and is more honest for
  multi-process scenarios; for single-process benchmarks RSS is fine. We may add USS as a
  secondary column later.
- **Caveat: RSS includes mmap-backed pages.** When DuckDB or delta-rs `mmap`s a Parquet file,
  those pages count against RSS even though the OS can reclaim them under pressure. So for
  read-heavy workloads, the reported RSS *can* exceed the engine's working set. This is honest
  (it's real memory pressure) but should be disclosed, especially for full-scan reads where most
  RSS is just file cache.
- **Caveat: 50 ms sampling can miss sub-50ms peaks.** For very fast operations (<100 ms), peak
  may be undersampled. Mitigation: tighten interval to 10 ms for the `tiny` and `small` profiles.
- **Caveat: warmup run is not measured but still allocates.** The first write/read warms the
  Python interpreter, lazy imports, and DuckDB extension loading. We discard its timing but the
  baseline RSS for subsequent measurements is *post-warmup*, which is correct.

## Storage size accounting

`BenchmarkResult` carries three fields:
- `disk_usage_mb` — bytes of data files (Parquet + Delta log / DuckLake data files).
- `postgres_metadata_mb` — DuckLake's catalog metadata size (Delta engine reports 0).
- `total_storage_mb` — derived sum.

### Postgres metadata is measured at DB granularity, not table

- We use `SELECT pg_database_size('benchmark_db_<storage_mode>')` minus an empty-DB baseline
  captured at engine setup.
- This includes catalog metadata for the bench table *plus* anything else in that DB. Because we
  use a dedicated DB per storage mode and clean up between runs, the residual is small but
  non-zero (Postgres internal tables, vacuum state).
- Implication: `postgres_metadata_mb` is an upper bound on the bench table's catalog cost, not
  a tight measurement. Adequate for "DuckLake catalog adds X MB on top of data files" claims;
  not adequate for sub-MB precision.

### Delta has no separate metadata storage

- All Delta metadata (`_delta_log/*.json`, checkpoints) lives alongside data files and is counted
  in `disk_usage_mb`. So `total_storage_mb` is directly comparable across engines, but
  `disk_usage_mb` alone is not (DuckLake's `disk_usage_mb` excludes catalog).

### Dict-encoded varchar in storage numbers

- We propagate `pa.dictionary(int32, string)` through to the engines (no `.cast(pa.string())` in
  `_gen_dict_string`). Both Parquet writers (Delta and DuckLake/DuckDB) honor this and write
  dictionary-encoded pages, which is one of DuckLake's selling points.
- Implication: storage numbers reflect dict encoding for `varchar_col` and `map`/`struct`
  varchar fields. If you compare to a naive plain-string benchmark, our numbers will look better
  for both engines.

## Merge streaming behavior

Both engines stream the merge source. There is no hidden materialization step on either side.

- **DuckLake**: `MERGE INTO ... USING <registered_arrow_reader>` consumes the
  `RecordBatchReader` lazily through DuckDB's Arrow scan operator. One catalog snapshot per
  call.
- **delta-rs (>=1.0)**: `DeltaTable.merge(source=record_batch_reader, ...)` accepts any
  `ArrowStreamExportable` and runs the merge with `streamed_exec=True` (the default) via
  DataFusion's `LazyMemoryExec` plan. Source batches are pulled lazily and the join workspace
  spills to disk via `max_spill_size` when memory pressure rises. One Delta version per call.
- The same code path is what Polars' `pl.LazyFrame.sink_delta(mode='merge')` wraps; using it
  would not change the streaming semantics, only the surface API.
- **Implication for the 100 GB headline:** `merge_upsert` is bounded by the join workspace
  (which spills), not by base-table or source-table size. Should hold on 16 GB RAM at the
  default `overlap_ratio=0.10`. If you do hit memory pressure, set `max_spill_size` explicitly
  via `delta_merge_options` in a custom variant.

## Live demo risks

- Laptop thermal throttling, OS background activity, screen-sharing CPU cost, network jitter
  (for S3 mode) all distort live numbers. Median-of-N helps but does not eliminate.
- **Recommendation:** before the talk, run the full benchmark on a clean machine, save the CSV,
  and have a fallback slide. Use the playground for live querying (more interactive, more
  forgiving). Run the live benchmark only on `tiny` or `small` profiles where the demo fits in a
  minute.
- The `quick`/`tiny` profile (10K rows) exists for this reason.

## What this benchmark does NOT measure

Disclose these on a single slide:

- ❌ Distributed/cluster performance.
- ❌ Concurrent writers, multi-version conflict resolution.
- ❌ Time-travel, version history performance, vacuum costs.
- ❌ Schema evolution operations (column add/drop/rename).
- ❌ Streaming ingest (continuous append from a producer); we measure batch ingest.
- ❌ Predicate pushdown effectiveness on nested types (struct/map filters).
- ❌ Cold-cache reads (we always warm with one run).
- ❌ Real S3 latency (MinIO is local-network; AWS S3 is 10-100x higher RTT).
- ❌ Catalog query throughput (we count its size, not its query cost).
- ❌ Delete/update workload (we cover insert + upsert via merge, not standalone deletes).

## Reproducibility

- Random seed is fixed via `GeneratorSpec.seed` (default 42). Same seed produces byte-identical
  Arrow tables across runs (modulo Polars/DuckDB internal nondeterminism in
  multi-threaded code paths).
- Engine setup uses dedicated Postgres DBs per storage mode, cleaned between runs — no
  cross-contamination.
- All knobs live in `config.yaml`. To reproduce, share the YAML and the `git rev-parse HEAD` of
  the repo.

## Talk disclosure language (suggested)

> "These numbers are from a single laptop, single process, with synthetic data. We measure
> one Delta version per write and one DuckLake snapshot per write — no batch-loop amplification.
> Memory is process RSS, which includes mmap'd file caches on read benchmarks. We pre-populated
> read tables once and ran each query 4 times (1 warmup, 3 timed). Merge benchmarks at 100M+ rows
> are bounded by Delta's all-in-memory merge source — that's a known limitation we'll come back
> to. Everything is in the repo with a single config file."
