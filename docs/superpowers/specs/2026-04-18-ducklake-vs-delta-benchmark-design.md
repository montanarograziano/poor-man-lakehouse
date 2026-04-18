# DuckLake v1.0 vs Delta-rs + Polars Benchmark — Design Spec

**Date**: 2026-04-18
**Status**: Approved

## Goal

Build a YAML-driven, modular benchmark framework comparing **DuckLake v1.0** (DuckDB extension + PostgreSQL metadata catalog) against **Delta-rs + Polars** across write, read, and merge operations. Run locally with tunable table sizes, on both local filesystem and MinIO S3 storage. Collect wall time, memory, disk usage, file count, throughput, and scan efficiency metrics.

## Project Structure

```
notebooks/
  benchmark/
    config.yaml                          # Benchmark parameters
    benchmark_ducklake_vs_delta.py       # Marimo notebook — orchestrator + display
    helpers/
      __init__.py
      config.py                          # YAML loader + BenchmarkConfig dataclass
      data_generator.py                  # Synthetic data generation
      metrics.py                         # Timer, memory, disk usage collectors + BenchmarkResult
      engines/
        __init__.py
        base.py                          # BenchmarkEngine Protocol
        ducklake_engine.py               # DuckLake implementation
        delta_engine.py                  # Delta-rs + Polars implementation
      results.py                         # Aggregation, pivot tables, CSV export
    data/                                # Local storage (gitignored)
    results/                             # CSV output (gitignored)
```

## Config Schema (config.yaml)

```yaml
benchmark:
  name: "DuckLake v1.0 vs Delta-rs + Polars"
  warmup_runs: 1
  repeat_runs: 3

table_sizes:
  small:    100_000
  medium:  1_000_000
  large:  10_000_000
  xl:    100_000_000

storage_modes:
  - local
  - s3

engines:
  - ducklake
  - delta

operations:
  write:
    - append
    - overwrite
  read:
    - full_scan
    - filtered_scan
    - aggregation
  merge:
    - upsert

schema:
  id_col: "id"
  columns:
    # Integers
    - { name: "int8_col",    type: "int8" }
    - { name: "int16_col",   type: "int16" }
    - { name: "int32_col",   type: "int32" }
    - { name: "int64_col",   type: "int64" }
    # Floats
    - { name: "float32_col", type: "float32" }
    - { name: "float64_col", type: "float64" }
    - { name: "decimal_col", type: "decimal", precision: 18, scale: 4 }
    # Temporal
    - { name: "date_col",       type: "date" }
    - { name: "datetime_col",   type: "datetime" }
    - { name: "timestamp_col",  type: "timestamp" }
    # String
    - { name: "varchar_col",    type: "varchar", cardinality: 1000 }
    - { name: "text_col",       type: "text", avg_length: 128 }
    # Boolean
    - { name: "bool_col",       type: "boolean" }
    # Complex
    - { name: "list_col",       type: "list", child_type: "int32", avg_length: 5 }
    - { name: "struct_col",     type: "struct", fields: ["a:int32", "b:varchar", "c:float64"] }
    - { name: "map_col",        type: "map", key_type: "varchar", value_type: "int32", avg_length: 3 }

postgres:
  host: "localhost"
  port: 5432
  database: "benchmark_db"
  user: "user"
  password: "password"

s3:
  endpoint: "http://localhost:9000"
  access_key: "minioadmin"
  secret_key: "miniopassword"
  bucket: "warehouse"
  ducklake_prefix: "benchmark/ducklake/"
  delta_prefix: "benchmark/delta/"

local:
  base_path: "./notebooks/benchmark/data/"
  ducklake_prefix: "ducklake/"
  delta_prefix: "delta/"

metrics:
  - wall_time_seconds
  - peak_memory_mb
  - disk_usage_mb
  - file_count
  - throughput_rows_per_sec
  - avg_file_size_mb
  - scan_efficiency_ratio
```

## Engine Abstraction

Both engines implement a `BenchmarkEngine` Protocol:

```python
class BenchmarkEngine(Protocol):
    name: str

    def setup(self, config: BenchmarkConfig, storage_mode: str) -> None: ...
    def write_append(self, table_name: str, data: pa.Table) -> None: ...
    def write_overwrite(self, table_name: str, data: pa.Table) -> None: ...
    def merge_upsert(self, table_name: str, source: pa.Table, merge_key: str) -> None: ...
    def read_full_scan(self, table_name: str) -> int: ...
    def read_filtered_scan(self, table_name: str) -> int: ...
    def read_aggregation(self, table_name: str) -> pa.Table: ...
    def get_disk_usage(self, table_name: str) -> tuple[int, int]: ...
    def teardown(self, table_name: str) -> None: ...
```

- Common interchange format: **PyArrow Tables**
- DuckLake engine: registers Arrow as DuckDB relation, uses SQL for all operations
- Delta engine: uses `write_deltalake()`, `DeltaTable.merge()`, Polars `scan_delta()`

### DuckLake Engine Details

- **Setup**: Connect to PostgreSQL, create `benchmark_db` if missing, create DuckDB connection, install/load ducklake + postgres extensions, create S3/postgres/ducklake secrets, ATTACH catalog
- **write_append**: `INSERT INTO {table} SELECT * FROM arrow_data`
- **write_overwrite**: `DROP TABLE IF EXISTS` + `CREATE TABLE AS SELECT`
- **merge_upsert**: `MERGE INTO {table} USING source ON (target.id = source.id) WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...`
- **read_full_scan**: `SELECT * FROM {table}` — materialize all rows/columns into Arrow, return row count. This forces a true full scan including deserialization of all column types.
- **read_filtered_scan**: `SELECT COUNT(*) FROM {table} WHERE date_col BETWEEN ... AND varchar_col IN (...)`
- **read_aggregation**: `SELECT varchar_col, COUNT(*), SUM(int64_col), AVG(float64_col), MIN(date_col), MAX(date_col) FROM {table} GROUP BY varchar_col`
- **get_disk_usage**: For local, walk the data directory. For S3, use MinIO client or `aws s3 ls --recursive` equivalent via DuckDB's `glob()` or `httpfs`
- **teardown**: `DROP TABLE`, clean data files

### Delta Engine Details

- **Setup**: Configure `storage_options` dict for S3 or local path
- **write_append**: `write_deltalake(data, mode="append")`
- **write_overwrite**: `write_deltalake(data, mode="overwrite")`
- **merge_upsert**: `DeltaTable.merge(source, predicate="s.id = t.id").when_matched_update_all().when_not_matched_insert_all().execute()`
- **read_full_scan**: `pl.scan_delta(path).collect()` — materialize all rows/columns, return row count
- **read_filtered_scan**: `pl.scan_delta(path).filter(...).select(pl.len()).collect()`
- **read_aggregation**: `pl.scan_delta(path).group_by("varchar_col").agg(...).collect()`
- **get_disk_usage**: Walk local directory or list S3 objects under the delta prefix
- **teardown**: Delete local files or S3 objects

## Metrics Collection

```python
@dataclass
class BenchmarkResult:
    engine: str                          # "ducklake" | "delta"
    operation: str                       # "write_append" | "read_full_scan" | ...
    storage_mode: str                    # "local" | "s3"
    table_size: str                      # "small" | "medium" | "large" | "xl"
    row_count: int
    wall_time_seconds: float             # Median of repeat_runs
    peak_memory_mb: float
    disk_usage_mb: float
    file_count: int
    throughput_rows_per_sec: float       # Derived: row_count / wall_time
    avg_file_size_mb: float              # Derived: disk_usage / file_count
    scan_efficiency_ratio: float | None  # Only for filtered reads
```

- **Wall time**: `time.perf_counter()` around the operation
- **Peak memory**: `tracemalloc.get_traced_memory()[1]` (peak)
- **Disk usage / file count**: Engine's `get_disk_usage()` after the operation completes
- **Throughput**: Derived from row_count / wall_time
- **Avg file size**: Derived from disk_usage / file_count
- **Scan efficiency**: For filtered reads only — ratio of filtered_scan time to full_scan time for same engine/size/storage

## Data Generation

### Main generator

`generate_data_batched(row_count, schema, batch_size=1_000_000, seed=42) -> Iterator[pa.RecordBatch]`

Produces Arrow RecordBatches in chunks. For sizes <= 1M, a convenience `generate_data()` wrapper materializes the full table.

### Column generation:

| Type | Strategy | Distribution |
|------|----------|-------------|
| id | Sequential 0..N-1 | Unique, monotonic |
| int8/16/32/64 | Random within type range | Uniform |
| float32/64 | Random uniform | -1e6..1e6 / -1e15..1e15 |
| decimal(18,4) | Random uniform, 4dp | -1e14..1e14 |
| date | Random 2020-01-01..2025-12-31 | Uniform |
| datetime | Same range, microsecond precision | Uniform |
| timestamp (tz) | Same as datetime + UTC | Uniform |
| varchar | Pick from pool of `cardinality` strings | Categorical |
| text | Random alphanumeric, normal dist around avg_length | Variable |
| boolean | 50/50 | Uniform |
| list\<int32\> | Random int32 lists, length Poisson(5) | Poisson length |
| struct | Dict with random values per field | Independent |
| map\<varchar, int32\> | 1-5 random kv pairs from key pool of 50 | Poisson size |

All randomness seeded for reproducibility.

### Merge data

`generate_merge_data(base_data, overlap_ratio=0.10, seed=43) -> pa.Table`

- 10% of rows: existing IDs from base, new random values (= updates)
- 90% of rows: new sequential IDs beyond base max (= inserts)
- Total merge source size = same as base table

### Filtered scan predicate (consistent across engines)

```sql
WHERE date_col BETWEEN '2023-01-01' AND '2023-12-31'
  AND varchar_col IN ('value_001', 'value_002', 'value_003')
```

Selects ~0.6% of rows.

### Aggregation query (consistent across engines)

```sql
SELECT varchar_col,
       COUNT(*) AS cnt,
       SUM(int64_col) AS sum_val,
       AVG(float64_col) AS avg_val,
       MIN(date_col) AS min_date,
       MAX(date_col) AS max_date
FROM {table}
GROUP BY varchar_col
```

Produces ~1000 result rows.

## Orchestration Loop

```
for size_name, row_count in config.table_sizes:
  data_batches = generate_data_batched(row_count, config.schema)
  merge_data = generate_merge_data(base_data_sample, overlap=0.10)
  for storage_mode in config.storage_modes:
    for engine in engines:
      engine.setup(config, storage_mode)
      for operation in all_operations:
        # Warmup (discarded)
        for _ in range(warmup_runs):
          run(engine, operation, data)
          engine.teardown()
        # Timed runs
        timings = []
        for _ in range(repeat_runs):
          pre_populate_if_needed(engine, operation, data)  # untimed
          result = measure(engine, operation, data)
          timings.append(result)
          engine.teardown()
        results.append(median(timings))
```

- Teardown between every run
- Pre-populate (untimed) before read/merge operations
- Median of timed runs

## Results Display

The notebook's final cells show:
1. **Raw results** — full Polars DataFrame with all metrics
2. **Pivot comparison** — DuckLake vs Delta side by side per operation/size/storage
3. **Speedup ratios** — `delta_time / ducklake_time` per operation
4. **Memory comparison** — peak memory per engine/operation/size
5. **Storage efficiency** — disk usage + file count comparison
6. **CSV export** — all results saved to `notebooks/benchmark/results/`

## Dependencies

Add to `pyproject.toml` dev group:
- `deltalake` — delta-rs Python bindings
- `pyyaml` — config loading

Already available:
- `duckdb` (via ibis-framework[duckdb]) — needs v1.5.2+
- `polars`, `pyarrow` — in project deps
- `tracemalloc` — stdlib
- `loguru` — already in project deps

## Infrastructure

- **PostgreSQL**: Reuse docker-compose `postgres_db` (port 5432). Create separate `benchmark_db` at startup (connect to `postgres` db first, check/create).
- **MinIO**: Reuse docker-compose service. Writes to `s3://warehouse/benchmark/` prefix.
- **Local**: `notebooks/benchmark/data/` directory, gitignored.
- **Results**: `notebooks/benchmark/results/` directory, gitignored.

## Risks & Mitigations

1. **DuckLake complex types** — list/struct/map documented but not battle-tested. If a complex type fails at write time, log warning and skip that column (don't abort the benchmark).
2. **XL tier memory** — 100M rows need chunked generation. If machine OOMs, gracefully skip XL with a logged warning.
3. **DuckDB version** — needs 1.5.2+. Check at startup, fail fast with clear message.
4. **PostgreSQL CREATE DATABASE** — no `IF NOT EXISTS` syntax. Connect to `postgres` db first, check `pg_database` catalog, create conditionally.
5. **delta-rs complex type support** — struct/list are supported via Arrow, map support may be partial. Same graceful skip strategy as DuckLake.

## Out of Scope

- No plotting libraries (matplotlib/plotly) — Polars tables are sufficient
- No CI integration — local interactive benchmark only
- No PySpark engine — could be added as a third engine later
- No partitioned writes — partitioning strategies differ too much between engines for fair comparison
