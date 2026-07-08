# DuckLake v1.0 vs Delta-rs Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a YAML-driven benchmark framework comparing DuckLake v1.0 against Delta-rs + Polars across write/read/merge operations at configurable table sizes on local and S3 storage.

**Architecture:** A marimo notebook orchestrates the benchmark loop, backed by a `helpers/` package with config loading, data generation, engine abstractions (DuckLake and Delta), metric collection, and results display. All parameters are controlled via `config.yaml`.

**Tech Stack:** Python 3.14, DuckDB 1.5.2 (ducklake extension), delta-rs (deltalake), Polars 1.40, PyArrow, marimo, loguru, pyyaml, PostgreSQL 17.5 (docker-compose), MinIO (docker-compose).

**Note:** User requested no git commits until explicitly asked. Skip all commit steps — stage nothing.

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `pyproject.toml` | Modify | Add `deltalake` to dev deps |
| `notebooks/benchmark/config.yaml` | Create | All tunable benchmark parameters |
| `notebooks/benchmark/helpers/__init__.py` | Create | Package re-exports |
| `notebooks/benchmark/helpers/config.py` | Create | YAML loader + `BenchmarkConfig` dataclass |
| `notebooks/benchmark/helpers/data_generator.py` | Create | Synthetic data generation (batched + full) |
| `notebooks/benchmark/helpers/metrics.py` | Create | Timer, memory, disk collectors + `BenchmarkResult` |
| `notebooks/benchmark/helpers/engines/__init__.py` | Create | Engine factory `get_engine()` |
| `notebooks/benchmark/helpers/engines/base.py` | Create | `BenchmarkEngine` Protocol |
| `notebooks/benchmark/helpers/engines/ducklake_engine.py` | Create | DuckLake DuckDB-based engine |
| `notebooks/benchmark/helpers/engines/delta_engine.py` | Create | Delta-rs + Polars engine |
| `notebooks/benchmark/helpers/results.py` | Create | Aggregation, pivots, CSV export |
| `notebooks/benchmark/benchmark_ducklake_vs_delta.py` | Create | Marimo notebook orchestrator |
| `.gitignore` | Modify | Add benchmark data/results dirs |

---

### Task 1: Add dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add deltalake to dev dependency group**

In `pyproject.toml`, add `deltalake` to the `[dependency-groups] dev` list. PyYAML is already available (ships with many packages). Add it explicitly for clarity.

```toml
[dependency-groups]
dev = [
    "commitizen>=4.8.2",
    "deltalake>=1.0.0",
    "deptry>=0.23.0",
    "ipykernel>=6.29.5",
    "mypy>=1.16.0",
    "pyright>=1.1.390",
    "prek>=0.2.12",
    "pyyaml>=6.0",
    "pytest>=8.4.0",
    "pytest-cov>=6.0.0",
    "ruff>=0.15.0",
    "types-requests>=2.32.4.20250611",
    "pyarrow-stubs>=20.0.0.20251215",
    "testcontainers[compose]>=4.14.2",
    "karva>=0.0.1a4",
]
```

- [ ] **Step 2: Sync dependencies**

Run: `uv sync --all-groups`

Expected: deltalake and pyyaml installed successfully.

- [ ] **Step 3: Verify imports**

Run: `uv run python -c "import deltalake; import yaml; print('OK')"`

Expected: `OK`

---

### Task 2: Create config.yaml

**Files:**
- Create: `notebooks/benchmark/config.yaml`

- [ ] **Step 1: Create the config file**

```yaml
# DuckLake v1.0 vs Delta-rs + Polars — Benchmark Configuration
# All parameters are tunable. Modify this file to adjust the benchmark scope.

benchmark:
  name: "DuckLake v1.0 vs Delta-rs + Polars"
  warmup_runs: 1          # Discarded runs before timing
  repeat_runs: 3          # Timed runs per operation (median taken)
  batch_size: 1_000_000   # Rows per batch for chunked generation

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

# Data generation schema
schema:
  id_col: "id"
  seed: 42
  merge_overlap_ratio: 0.10
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

# Filtered scan predicate (used consistently across engines)
filter:
  date_range: ["2023-01-01", "2023-12-31"]
  varchar_values: ["value_001", "value_002", "value_003"]

# Infrastructure
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

---

### Task 3: Create helpers/config.py

**Files:**
- Create: `notebooks/benchmark/helpers/__init__.py` (empty for now)
- Create: `notebooks/benchmark/helpers/config.py`

- [ ] **Step 1: Create the helpers package init**

```python
"""Benchmark helpers package."""
```

- [ ] **Step 2: Create config.py with BenchmarkConfig dataclass**

```python
"""YAML config loader and BenchmarkConfig dataclass."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class ColumnDef:
    """Column definition from the schema config."""

    name: str
    type: str
    # Optional type-specific parameters
    precision: int = 0
    scale: int = 0
    cardinality: int = 0
    avg_length: int = 0
    child_type: str = ""
    fields: list[str] = field(default_factory=list)
    key_type: str = ""
    value_type: str = ""


@dataclass(frozen=True)
class SchemaConfig:
    """Schema configuration for data generation."""

    id_col: str
    seed: int
    merge_overlap_ratio: float
    columns: list[ColumnDef]


@dataclass(frozen=True)
class FilterConfig:
    """Predicate config for filtered scans."""

    date_range: tuple[str, str]
    varchar_values: list[str]


@dataclass(frozen=True)
class PostgresConfig:
    """PostgreSQL connection config."""

    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class S3Config:
    """S3/MinIO config."""

    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    ducklake_prefix: str
    delta_prefix: str


@dataclass(frozen=True)
class LocalConfig:
    """Local filesystem config."""

    base_path: str
    ducklake_prefix: str
    delta_prefix: str


@dataclass(frozen=True)
class OperationsConfig:
    """Operations to benchmark."""

    write: list[str]
    read: list[str]
    merge: list[str]

    @property
    def all_operations(self) -> list[str]:
        """Return flat list of all operation names with category prefix."""
        ops: list[str] = []
        for op in self.write:
            ops.append(f"write_{op}")
        for op in self.read:
            ops.append(f"read_{op}")
        for op in self.merge:
            ops.append(f"merge_{op}")
        return ops


@dataclass(frozen=True)
class BenchmarkConfig:
    """Top-level benchmark configuration."""

    name: str
    warmup_runs: int
    repeat_runs: int
    batch_size: int
    table_sizes: dict[str, int]
    storage_modes: list[str]
    engines: list[str]
    operations: OperationsConfig
    schema: SchemaConfig
    filter: FilterConfig
    postgres: PostgresConfig
    s3: S3Config
    local: LocalConfig


def load_config(config_path: str | Path) -> BenchmarkConfig:
    """Load benchmark configuration from YAML file.

    Args:
        config_path: Path to the config.yaml file.

    Returns:
        Parsed BenchmarkConfig instance.
    """
    path = Path(config_path)
    with path.open() as f:
        raw = yaml.safe_load(f)

    columns = [ColumnDef(**col) for col in raw["schema"]["columns"]]

    schema = SchemaConfig(
        id_col=raw["schema"]["id_col"],
        seed=raw["schema"]["seed"],
        merge_overlap_ratio=raw["schema"]["merge_overlap_ratio"],
        columns=columns,
    )

    filter_cfg = FilterConfig(
        date_range=tuple(raw["filter"]["date_range"]),
        varchar_values=raw["filter"]["varchar_values"],
    )

    operations = OperationsConfig(
        write=raw["operations"]["write"],
        read=raw["operations"]["read"],
        merge=raw["operations"]["merge"],
    )

    pg = raw["postgres"]
    postgres = PostgresConfig(
        host=pg["host"], port=pg["port"], database=pg["database"],
        user=pg["user"], password=pg["password"],
    )

    s3_raw = raw["s3"]
    s3 = S3Config(
        endpoint=s3_raw["endpoint"], access_key=s3_raw["access_key"],
        secret_key=s3_raw["secret_key"], bucket=s3_raw["bucket"],
        ducklake_prefix=s3_raw["ducklake_prefix"], delta_prefix=s3_raw["delta_prefix"],
    )

    local_raw = raw["local"]
    local = LocalConfig(
        base_path=local_raw["base_path"],
        ducklake_prefix=local_raw["ducklake_prefix"],
        delta_prefix=local_raw["delta_prefix"],
    )

    bench = raw["benchmark"]
    return BenchmarkConfig(
        name=bench["name"],
        warmup_runs=bench["warmup_runs"],
        repeat_runs=bench["repeat_runs"],
        batch_size=bench["batch_size"],
        table_sizes=raw["table_sizes"],
        storage_modes=raw["storage_modes"],
        engines=raw["engines"],
        operations=operations,
        schema=schema,
        filter=filter_cfg,
        postgres=postgres,
        s3=s3,
        local=local,
    )
```

- [ ] **Step 3: Verify config loads**

Run: `uv run python -c "from notebooks.benchmark.helpers.config import load_config; cfg = load_config('notebooks/benchmark/config.yaml'); print(cfg.name, len(cfg.schema.columns))"`

Expected: `DuckLake v1.0 vs Delta-rs + Polars 16`

---

### Task 4: Create helpers/metrics.py

**Files:**
- Create: `notebooks/benchmark/helpers/metrics.py`

- [ ] **Step 1: Create metrics.py with timer, memory tracker, and BenchmarkResult**

```python
"""Metric collection utilities for benchmarking."""

from __future__ import annotations

import os
import time
import tracemalloc
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from loguru import logger


@dataclass
class BenchmarkResult:
    """Single benchmark measurement."""

    engine: str
    operation: str
    storage_mode: str
    table_size: str
    row_count: int
    wall_time_seconds: float
    peak_memory_mb: float
    disk_usage_mb: float
    file_count: int
    throughput_rows_per_sec: float
    avg_file_size_mb: float
    scan_efficiency_ratio: float | None = None


@dataclass
class TimingResult:
    """Raw timing + memory from a single run."""

    wall_time_seconds: float
    peak_memory_mb: float


@contextmanager
def measure_time_and_memory() -> Iterator[list[TimingResult]]:
    """Context manager that measures wall time and peak memory.

    Usage:
        with measure_time_and_memory() as results:
            do_something()
        timing = results[0]
    """
    container: list[TimingResult] = []
    tracemalloc.start()
    # Reset peak by taking a snapshot
    tracemalloc.reset_peak()
    start = time.perf_counter()
    yield container
    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    container.append(TimingResult(
        wall_time_seconds=elapsed,
        peak_memory_mb=peak / (1024 * 1024),
    ))
    logger.debug(f"Elapsed: {elapsed:.3f}s | Peak memory: {peak / (1024 * 1024):.1f} MB")


def get_local_disk_usage(path: str) -> tuple[int, int]:
    """Get total bytes and file count for a local directory.

    Args:
        path: Directory path to measure.

    Returns:
        Tuple of (total_bytes, file_count).
    """
    total_bytes = 0
    file_count = 0
    target = os.path.abspath(path)
    if not os.path.exists(target):
        return 0, 0
    for dirpath, _dirnames, filenames in os.walk(target):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total_bytes += os.path.getsize(fp)
                file_count += 1
    return total_bytes, file_count


def get_s3_disk_usage(*, bucket: str, prefix: str, endpoint: str, access_key: str, secret_key: str) -> tuple[int, int]:
    """Get total bytes and file count for an S3 prefix using DuckDB's httpfs.

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix to measure.
        endpoint: S3 endpoint URL.
        access_key: AWS access key.
        secret_key: AWS secret key.

    Returns:
        Tuple of (total_bytes, file_count).
    """
    import duckdb

    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE SECRET s3_measure (
            TYPE S3,
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{endpoint.replace("http://", "").replace("https://", "")}',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)
    s3_path = f"s3://{bucket}/{prefix}**"
    try:
        result = con.execute(f"""
            SELECT COALESCE(SUM(size), 0) AS total_bytes, COUNT(*) AS file_count
            FROM glob('{s3_path}')
        """).fetchone()
        con.close()
        if result:
            return int(result[0]), int(result[1])
    except Exception:
        logger.warning(f"Could not measure S3 disk usage for {s3_path}")
        con.close()
    return 0, 0


def build_result(
    *,
    engine: str,
    operation: str,
    storage_mode: str,
    table_size: str,
    row_count: int,
    timing: TimingResult,
    disk_bytes: int,
    file_count: int,
    full_scan_time: float | None = None,
) -> BenchmarkResult:
    """Build a BenchmarkResult from raw measurements.

    Args:
        engine: Engine name.
        operation: Operation name.
        storage_mode: Storage mode.
        table_size: Table size label.
        row_count: Number of rows.
        timing: Timing measurement.
        disk_bytes: Total disk bytes.
        file_count: Number of data files.
        full_scan_time: Full scan time for computing scan efficiency (filtered reads only).

    Returns:
        Populated BenchmarkResult.
    """
    disk_mb = disk_bytes / (1024 * 1024)
    avg_file_mb = disk_mb / file_count if file_count > 0 else 0.0
    throughput = row_count / timing.wall_time_seconds if timing.wall_time_seconds > 0 else 0.0
    scan_eff = None
    if full_scan_time is not None and full_scan_time > 0:
        scan_eff = timing.wall_time_seconds / full_scan_time

    return BenchmarkResult(
        engine=engine,
        operation=operation,
        storage_mode=storage_mode,
        table_size=table_size,
        row_count=row_count,
        wall_time_seconds=round(timing.wall_time_seconds, 4),
        peak_memory_mb=round(timing.peak_memory_mb, 2),
        disk_usage_mb=round(disk_mb, 2),
        file_count=file_count,
        throughput_rows_per_sec=round(throughput, 1),
        avg_file_size_mb=round(avg_file_mb, 2),
        scan_efficiency_ratio=round(scan_eff, 4) if scan_eff is not None else None,
    )
```

---

### Task 5: Create helpers/engines/base.py

**Files:**
- Create: `notebooks/benchmark/helpers/engines/__init__.py`
- Create: `notebooks/benchmark/helpers/engines/base.py`

- [ ] **Step 1: Create engines package init (placeholder)**

```python
"""Benchmark engine implementations."""
```

- [ ] **Step 2: Create BenchmarkEngine Protocol**

```python
"""Abstract engine protocol for benchmark implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import pyarrow as pa

    from notebooks.benchmark.helpers.config import BenchmarkConfig


@runtime_checkable
class BenchmarkEngine(Protocol):
    """Protocol defining the benchmark engine interface.

    Both DuckLake and Delta engines must implement all methods.
    Data is exchanged as PyArrow Tables for a fair common format.
    """

    name: str

    def setup(self, config: BenchmarkConfig, storage_mode: str) -> None:
        """Initialize connections, create catalog/secrets, ensure clean state."""
        ...

    def write_append(self, table_name: str, data: pa.Table) -> None:
        """Append rows to an existing table, or create it if it doesn't exist."""
        ...

    def write_overwrite(self, table_name: str, data: pa.Table) -> None:
        """Overwrite the table entirely with new data."""
        ...

    def merge_upsert(self, table_name: str, source: pa.Table, merge_key: str) -> None:
        """Upsert: update rows matching on merge_key, insert non-matching rows."""
        ...

    def read_full_scan(self, table_name: str) -> int:
        """Full table scan materializing all columns. Return row count."""
        ...

    def read_filtered_scan(self, table_name: str) -> int:
        """Filtered scan with predicate pushdown. Return row count."""
        ...

    def read_aggregation(self, table_name: str) -> pa.Table:
        """GROUP BY aggregation query. Return result as Arrow table."""
        ...

    def get_disk_usage(self, table_name: str) -> tuple[int, int]:
        """Return (total_bytes, file_count) for the table's data files."""
        ...

    def teardown(self, table_name: str) -> None:
        """Drop the table and clean up all data files."""
        ...

    def close(self) -> None:
        """Release connections and resources."""
        ...
```

---

### Task 6: Create helpers/data_generator.py

**Files:**
- Create: `notebooks/benchmark/helpers/data_generator.py`

- [ ] **Step 1: Create the data generator**

```python
"""Synthetic data generation for benchmarking."""

from __future__ import annotations

import datetime
import random
import string
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
from loguru import logger

from notebooks.benchmark.helpers.config import ColumnDef, SchemaConfig


def _random_string(length: int, rng: random.Random) -> str:
    return "".join(rng.choices(string.ascii_lowercase + string.digits, k=length))


def _build_string_pool(prefix: str, count: int) -> list[str]:
    return [f"{prefix}_{i:03d}" for i in range(count)]


def _generate_column(col: ColumnDef, row_count: int, rng: random.Random) -> pa.Array:
    """Generate a single column's data as a PyArrow array."""
    if col.type == "int8":
        data = [rng.randint(-128, 127) for _ in range(row_count)]
        return pa.array(data, type=pa.int8())

    if col.type == "int16":
        data = [rng.randint(-32768, 32767) for _ in range(row_count)]
        return pa.array(data, type=pa.int16())

    if col.type == "int32":
        data = [rng.randint(-(2**31), 2**31 - 1) for _ in range(row_count)]
        return pa.array(data, type=pa.int32())

    if col.type == "int64":
        data = [rng.randint(-(2**55), 2**55 - 1) for _ in range(row_count)]
        return pa.array(data, type=pa.int64())

    if col.type == "float32":
        data = [rng.uniform(-1e6, 1e6) for _ in range(row_count)]
        return pa.array(data, type=pa.float32())

    if col.type == "float64":
        data = [rng.uniform(-1e15, 1e15) for _ in range(row_count)]
        return pa.array(data, type=pa.float64())

    if col.type == "decimal":
        scale = col.scale or 4
        precision = col.precision or 18
        max_val = 10 ** (precision - scale)
        data = [round(rng.uniform(-max_val, max_val), scale) for _ in range(row_count)]
        return pa.array(data, type=pa.decimal128(precision, scale))

    if col.type == "date":
        start = datetime.date(2020, 1, 1)
        days_range = (datetime.date(2025, 12, 31) - start).days
        data = [start + datetime.timedelta(days=rng.randint(0, days_range)) for _ in range(row_count)]
        return pa.array(data, type=pa.date32())

    if col.type == "datetime":
        start_ts = datetime.datetime(2020, 1, 1, tzinfo=None)
        end_ts = datetime.datetime(2025, 12, 31, 23, 59, 59, tzinfo=None)
        delta = (end_ts - start_ts).total_seconds()
        data = [start_ts + datetime.timedelta(seconds=rng.uniform(0, delta)) for _ in range(row_count)]
        return pa.array(data, type=pa.timestamp("us"))

    if col.type == "timestamp":
        start_ts = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
        end_ts = datetime.datetime(2025, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
        delta = (end_ts - start_ts).total_seconds()
        data = [start_ts + datetime.timedelta(seconds=rng.uniform(0, delta)) for _ in range(row_count)]
        return pa.array(data, type=pa.timestamp("us", tz="UTC"))

    if col.type == "varchar":
        pool = _build_string_pool("value", col.cardinality or 1000)
        data = [rng.choice(pool) for _ in range(row_count)]
        return pa.array(data, type=pa.string())

    if col.type == "text":
        avg_len = col.avg_length or 128
        data = [_random_string(max(1, int(rng.gauss(avg_len, avg_len * 0.3))), rng) for _ in range(row_count)]
        return pa.array(data, type=pa.large_string())

    if col.type == "boolean":
        data = [rng.choice([True, False]) for _ in range(row_count)]
        return pa.array(data, type=pa.bool_())

    if col.type == "list":
        child = pa.int32()
        data = [
            [rng.randint(-(2**31), 2**31 - 1) for _ in range(max(1, int(rng.gauss(col.avg_length or 5, 2))))]
            for _ in range(row_count)
        ]
        return pa.array(data, type=pa.list_(child))

    if col.type == "struct":
        # Parse field definitions like ["a:int32", "b:varchar", "c:float64"]
        field_arrays: dict[str, pa.Array] = {}
        struct_fields: list[pa.Field] = []
        for field_def in col.fields:
            fname, ftype = field_def.split(":")
            if ftype == "int32":
                arr = pa.array([rng.randint(-(2**31), 2**31 - 1) for _ in range(row_count)], type=pa.int32())
                struct_fields.append(pa.field(fname, pa.int32()))
            elif ftype == "varchar":
                arr = pa.array([_random_string(10, rng) for _ in range(row_count)], type=pa.string())
                struct_fields.append(pa.field(fname, pa.string()))
            elif ftype == "float64":
                arr = pa.array([rng.uniform(-1e6, 1e6) for _ in range(row_count)], type=pa.float64())
                struct_fields.append(pa.field(fname, pa.float64()))
            else:
                msg = f"Unsupported struct field type: {ftype}"
                raise ValueError(msg)
            field_arrays[fname] = arr
        return pa.StructArray.from_arrays(
            list(field_arrays.values()),
            fields=struct_fields,
        )

    if col.type == "map":
        key_pool = _build_string_pool("key", 50)
        avg_len = col.avg_length or 3
        keys_list: list[list[str]] = []
        values_list: list[list[int]] = []
        for _ in range(row_count):
            n = max(1, int(rng.gauss(avg_len, 1)))
            selected_keys = rng.sample(key_pool, min(n, len(key_pool)))
            keys_list.append(selected_keys)
            values_list.append([rng.randint(-1000, 1000) for _ in selected_keys])
        # Build as list of (key, value) tuples then convert
        map_data: list[list[tuple[str, int]]] = [
            list(zip(ks, vs)) for ks, vs in zip(keys_list, values_list)
        ]
        return pa.array(map_data, type=pa.map_(pa.string(), pa.int32()))

    msg = f"Unsupported column type: {col.type}"
    raise ValueError(msg)


def generate_batch(
    schema_config: SchemaConfig,
    row_count: int,
    id_offset: int = 0,
    seed: int = 42,
) -> pa.Table:
    """Generate a single batch of synthetic data.

    Args:
        schema_config: Schema configuration with column definitions.
        row_count: Number of rows to generate.
        id_offset: Starting ID value.
        seed: Random seed for reproducibility.

    Returns:
        PyArrow Table with the generated data.
    """
    rng = random.Random(seed)
    arrays: dict[str, pa.Array] = {}

    # ID column — sequential
    arrays[schema_config.id_col] = pa.array(
        list(range(id_offset, id_offset + row_count)), type=pa.int64()
    )

    # Data columns
    for col in schema_config.columns:
        try:
            arrays[col.name] = _generate_column(col, row_count, rng)
        except Exception:
            logger.warning(f"Failed to generate column {col.name} ({col.type}), skipping")

    return pa.table(arrays)


def generate_data_batched(
    schema_config: SchemaConfig,
    total_rows: int,
    batch_size: int = 1_000_000,
    seed: int = 42,
) -> Iterator[pa.Table]:
    """Generate data in batches for memory-efficient processing.

    Args:
        schema_config: Schema configuration.
        total_rows: Total number of rows to generate.
        batch_size: Rows per batch.
        seed: Base random seed. Each batch uses seed + batch_index.

    Yields:
        PyArrow Tables, one per batch.
    """
    generated = 0
    batch_idx = 0
    while generated < total_rows:
        chunk_size = min(batch_size, total_rows - generated)
        logger.info(f"Generating batch {batch_idx}: rows {generated}..{generated + chunk_size - 1}")
        yield generate_batch(
            schema_config,
            row_count=chunk_size,
            id_offset=generated,
            seed=seed + batch_idx,
        )
        generated += chunk_size
        batch_idx += 1
    logger.info(f"Data generation complete: {total_rows} rows in {batch_idx} batches")


def generate_data(schema_config: SchemaConfig, total_rows: int, seed: int = 42) -> pa.Table:
    """Generate the full dataset as a single PyArrow Table.

    For large datasets (>1M rows), prefer generate_data_batched.

    Args:
        schema_config: Schema configuration.
        total_rows: Number of rows.
        seed: Random seed.

    Returns:
        Single PyArrow Table with all rows.
    """
    batches = list(generate_data_batched(schema_config, total_rows, batch_size=total_rows, seed=seed))
    return batches[0]


def generate_merge_data(
    base_data: pa.Table,
    id_col: str,
    schema_config: SchemaConfig,
    overlap_ratio: float = 0.10,
    seed: int = 43,
) -> pa.Table:
    """Generate merge/upsert source data with partial overlap.

    Args:
        base_data: The existing table data.
        id_col: Name of the ID column used as merge key.
        schema_config: Schema config for generating new column values.
        overlap_ratio: Fraction of base rows to update (0.10 = 10%).
        seed: Random seed.

    Returns:
        PyArrow Table with update rows (existing IDs) + insert rows (new IDs).
    """
    rng = random.Random(seed)
    base_ids = base_data.column(id_col).to_pylist()
    total_rows = len(base_ids)

    # Rows to update: sample existing IDs
    update_count = int(total_rows * overlap_ratio)
    update_ids = rng.sample(base_ids, min(update_count, len(base_ids)))

    # Rows to insert: new IDs starting after max
    insert_count = total_rows - update_count
    max_id = max(base_ids)
    insert_ids = list(range(max_id + 1, max_id + 1 + insert_count))

    all_ids = update_ids + insert_ids
    merge_row_count = len(all_ids)

    # Generate fresh data for all non-ID columns
    fresh = generate_batch(schema_config, merge_row_count, seed=seed + 100)

    # Replace the ID column with our crafted IDs
    arrays: dict[str, Any] = {id_col: pa.array(all_ids, type=pa.int64())}
    for col_name in fresh.column_names:
        if col_name != id_col:
            arrays[col_name] = fresh.column(col_name)

    logger.info(f"Merge data: {update_count} updates + {insert_count} inserts = {merge_row_count} total")
    return pa.table(arrays)
```

- [ ] **Step 2: Verify data generation**

Run: `uv run python -c "
from notebooks.benchmark.helpers.config import load_config
from notebooks.benchmark.helpers.data_generator import generate_data
cfg = load_config('notebooks/benchmark/config.yaml')
t = generate_data(cfg.schema, 1000)
print(f'Rows: {t.num_rows}, Cols: {t.num_columns}')
print(t.schema)
"`

Expected: `Rows: 1000, Cols: 17` and a printed schema with all column types.

---

### Task 7: Create helpers/engines/ducklake_engine.py

**Files:**
- Create: `notebooks/benchmark/helpers/engines/ducklake_engine.py`

- [ ] **Step 1: Create the DuckLake engine**

```python
"""DuckLake benchmark engine using DuckDB + PostgreSQL metadata catalog."""

from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
from loguru import logger

if TYPE_CHECKING:
    from notebooks.benchmark.helpers.config import BenchmarkConfig, FilterConfig

_MIN_DUCKDB_VERSION = "1.5.2"


class DuckLakeEngine:
    """Benchmark engine for DuckLake v1.0.

    Uses DuckDB's ducklake extension with PostgreSQL as the metadata catalog.
    Data is stored as Parquet files on local filesystem or S3/MinIO.
    """

    name: str = "ducklake"

    def __init__(self) -> None:
        self._con: duckdb.DuckDBPyConnection | None = None
        self._config: BenchmarkConfig | None = None
        self._storage_mode: str = ""
        self._data_path: str = ""
        self._catalog_name: str = "bench_ducklake"
        self._filter: FilterConfig | None = None

    def setup(self, config: BenchmarkConfig, storage_mode: str) -> None:
        """Initialize DuckDB connection, install extensions, create secrets, attach DuckLake."""
        self._config = config
        self._storage_mode = storage_mode
        self._filter = config.filter

        # Version check
        version = duckdb.__version__
        if version < _MIN_DUCKDB_VERSION:
            msg = f"DuckDB {_MIN_DUCKDB_VERSION}+ required, got {version}"
            raise RuntimeError(msg)

        self._con = duckdb.connect()

        # Install and load extensions
        self._con.execute("INSTALL ducklake; INSTALL postgres;")
        self._con.execute("LOAD ducklake; LOAD postgres;")

        pg = config.postgres

        if storage_mode == "s3":
            s3 = config.s3
            self._data_path = f"s3://{s3.bucket}/{s3.ducklake_prefix}"
            endpoint_stripped = s3.endpoint.replace("http://", "").replace("https://", "")
            self._con.execute(f"""
                CREATE OR REPLACE SECRET s3_secret (
                    TYPE S3,
                    KEY_ID '{s3.access_key}',
                    SECRET '{s3.secret_key}',
                    ENDPOINT '{endpoint_stripped}',
                    URL_STYLE 'path',
                    USE_SSL false
                );
            """)
        else:
            base = os.path.abspath(config.local.base_path)
            self._data_path = os.path.join(base, config.local.ducklake_prefix)
            os.makedirs(self._data_path, exist_ok=True)

        # Ensure benchmark_db exists in PostgreSQL
        self._ensure_postgres_db(pg.host, pg.port, pg.user, pg.password, pg.database)

        # Create postgres secret and attach DuckLake
        self._con.execute(f"""
            CREATE OR REPLACE SECRET postgres_secret (
                TYPE postgres,
                HOST '{pg.host}',
                PORT {pg.port},
                DATABASE '{pg.database}',
                USER '{pg.user}',
                PASSWORD '{pg.password}'
            );
        """)

        attach_uri = (
            f"ducklake:postgres:dbname={pg.database} host={pg.host} "
            f"port={pg.port} user={pg.user} password={pg.password}"
        )
        self._con.execute(f"""
            ATTACH OR REPLACE '{attach_uri}' AS {self._catalog_name}
                (DATA_PATH '{self._data_path}');
        """)
        self._con.execute(f"USE {self._catalog_name};")
        logger.info(f"DuckLake engine setup complete (storage={storage_mode}, data_path={self._data_path})")

    def _ensure_postgres_db(self, host: str, port: int, user: str, password: str, database: str) -> None:
        """Create the benchmark database in PostgreSQL if it doesn't exist."""
        import subprocess

        # Use psql to check/create. This avoids a psycopg2 dependency.
        check = subprocess.run(
            ["psql", "-h", host, "-p", str(port), "-U", user, "-lqt"],
            capture_output=True, text=True,
            env={**os.environ, "PGPASSWORD": password},
        )
        if database not in check.stdout:
            subprocess.run(
                ["psql", "-h", host, "-p", str(port), "-U", user,
                 "-c", f"CREATE DATABASE {database};"],
                capture_output=True, text=True,
                env={**os.environ, "PGPASSWORD": password},
            )
            logger.info(f"Created PostgreSQL database: {database}")

    def _qualified(self, table_name: str) -> str:
        return f"{self._catalog_name}.main.{table_name}"

    def write_append(self, table_name: str, data: pa.Table) -> None:
        """Append data. Creates table on first call."""
        assert self._con is not None
        self._con.register("_arrow_src", data)
        fq = self._qualified(table_name)
        try:
            self._con.execute(f"SELECT 1 FROM {fq} LIMIT 0")
            # Table exists — append
            self._con.execute(f"INSERT INTO {fq} SELECT * FROM _arrow_src")
        except duckdb.CatalogException:
            # Table doesn't exist — create
            self._con.execute(f"CREATE TABLE {fq} AS SELECT * FROM _arrow_src")
        finally:
            self._con.unregister("_arrow_src")

    def write_overwrite(self, table_name: str, data: pa.Table) -> None:
        """Drop and recreate the table with new data."""
        assert self._con is not None
        fq = self._qualified(table_name)
        self._con.register("_arrow_src", data)
        try:
            self._con.execute(f"DROP TABLE IF EXISTS {fq}")
            self._con.execute(f"CREATE TABLE {fq} AS SELECT * FROM _arrow_src")
        finally:
            self._con.unregister("_arrow_src")

    def merge_upsert(self, table_name: str, source: pa.Table, merge_key: str) -> None:
        """MERGE INTO with upsert semantics."""
        assert self._con is not None
        fq = self._qualified(table_name)
        self._con.register("_merge_src", source)

        # Build SET clause for all columns except the merge key
        non_key_cols = [c for c in source.column_names if c != merge_key]
        set_clause = ", ".join(f"target.{c} = source.{c}" for c in non_key_cols)
        insert_cols = ", ".join(source.column_names)
        insert_vals = ", ".join(f"source.{c}" for c in source.column_names)

        sql = f"""
            MERGE INTO {fq} AS target
            USING _merge_src AS source
            ON target.{merge_key} = source.{merge_key}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        try:
            self._con.execute(sql)
        finally:
            self._con.unregister("_merge_src")

    def read_full_scan(self, table_name: str) -> int:
        """Full table scan, materialize all columns."""
        assert self._con is not None
        fq = self._qualified(table_name)
        result = self._con.execute(f"SELECT * FROM {fq}").fetchall()
        return len(result)

    def read_filtered_scan(self, table_name: str) -> int:
        """Filtered scan with predicate pushdown."""
        assert self._con is not None
        assert self._filter is not None
        fq = self._qualified(table_name)
        date_start, date_end = self._filter.date_range
        varchar_vals = ", ".join(f"'{v}'" for v in self._filter.varchar_values)
        sql = f"""
            SELECT * FROM {fq}
            WHERE date_col BETWEEN '{date_start}' AND '{date_end}'
              AND varchar_col IN ({varchar_vals})
        """
        result = self._con.execute(sql).fetchall()
        return len(result)

    def read_aggregation(self, table_name: str) -> pa.Table:
        """Aggregation query."""
        assert self._con is not None
        fq = self._qualified(table_name)
        sql = f"""
            SELECT varchar_col,
                   COUNT(*) AS cnt,
                   SUM(int64_col) AS sum_val,
                   AVG(float64_col) AS avg_val,
                   MIN(date_col) AS min_date,
                   MAX(date_col) AS max_date
            FROM {fq}
            GROUP BY varchar_col
        """
        return self._con.execute(sql).fetch_arrow_table()

    def get_disk_usage(self, table_name: str) -> tuple[int, int]:
        """Measure disk usage of the table's data files."""
        assert self._config is not None
        if self._storage_mode == "s3":
            from notebooks.benchmark.helpers.metrics import get_s3_disk_usage
            s3 = self._config.s3
            return get_s3_disk_usage(
                bucket=s3.bucket, prefix=s3.ducklake_prefix,
                endpoint=s3.endpoint, access_key=s3.access_key, secret_key=s3.secret_key,
            )
        from notebooks.benchmark.helpers.metrics import get_local_disk_usage
        return get_local_disk_usage(self._data_path)

    def teardown(self, table_name: str) -> None:
        """Drop the table."""
        if self._con is None:
            return
        fq = self._qualified(table_name)
        try:
            self._con.execute(f"DROP TABLE IF EXISTS {fq}")
        except Exception:
            logger.warning(f"Failed to drop table {fq}")

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._con is not None:
            try:
                self._con.execute("USE memory;")
                self._con.execute(f"DETACH IF EXISTS {self._catalog_name};")
            except Exception:
                pass
            self._con.close()
            self._con = None

        # Clean local data if it was local storage
        if self._storage_mode == "local" and self._data_path and os.path.exists(self._data_path):
            shutil.rmtree(self._data_path, ignore_errors=True)
```

---

### Task 8: Create helpers/engines/delta_engine.py

**Files:**
- Create: `notebooks/benchmark/helpers/engines/delta_engine.py`

- [ ] **Step 1: Create the Delta engine**

```python
"""Delta-rs + Polars benchmark engine."""

from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from loguru import logger

if TYPE_CHECKING:
    from notebooks.benchmark.helpers.config import BenchmarkConfig, FilterConfig


class DeltaEngine:
    """Benchmark engine for Delta Lake using delta-rs and Polars.

    Writes via delta-rs write_deltalake(), reads via Polars scan_delta(),
    merges via DeltaTable.merge().
    """

    name: str = "delta"

    def __init__(self) -> None:
        self._config: BenchmarkConfig | None = None
        self._storage_mode: str = ""
        self._base_path: str = ""
        self._storage_options: dict[str, str] = {}
        self._filter: FilterConfig | None = None

    def _table_path(self, table_name: str) -> str:
        return f"{self._base_path}{table_name}"

    def setup(self, config: BenchmarkConfig, storage_mode: str) -> None:
        """Configure storage paths and options."""
        self._config = config
        self._storage_mode = storage_mode
        self._filter = config.filter

        if storage_mode == "s3":
            s3 = config.s3
            self._base_path = f"s3://{s3.bucket}/{s3.delta_prefix}"
            self._storage_options = {
                "AWS_ACCESS_KEY_ID": s3.access_key,
                "AWS_SECRET_ACCESS_KEY": s3.secret_key,
                "AWS_ENDPOINT_URL": s3.endpoint,
                "AWS_REGION": "us-east-1",
                "AWS_ALLOW_HTTP": "true",
                "allow_http": "true",
                "aws_conditional_put": "etag",
            }
        else:
            base = os.path.abspath(config.local.base_path)
            self._base_path = os.path.join(base, config.local.delta_prefix)
            os.makedirs(self._base_path, exist_ok=True)
            self._storage_options = {}

        logger.info(f"Delta engine setup complete (storage={storage_mode}, path={self._base_path})")

    def write_append(self, table_name: str, data: pa.Table) -> None:
        """Append data to delta table."""
        path = self._table_path(table_name)
        write_deltalake(
            table_or_uri=path,
            data=data,
            mode="append",
            storage_options=self._storage_options or None,
            engine="rust",
        )

    def write_overwrite(self, table_name: str, data: pa.Table) -> None:
        """Overwrite delta table with new data."""
        path = self._table_path(table_name)
        write_deltalake(
            table_or_uri=path,
            data=data,
            mode="overwrite",
            storage_options=self._storage_options or None,
            engine="rust",
        )

    def merge_upsert(self, table_name: str, source: pa.Table, merge_key: str) -> None:
        """Upsert via DeltaTable.merge()."""
        path = self._table_path(table_name)
        dt = DeltaTable(path, storage_options=self._storage_options or None)
        dt.merge(
            source=source,
            source_alias="source",
            target_alias="target",
            predicate=f"source.{merge_key} = target.{merge_key}",
        ).when_matched_update_all().when_not_matched_insert_all().execute()

    def read_full_scan(self, table_name: str) -> int:
        """Full table scan via Polars."""
        path = self._table_path(table_name)
        df = pl.scan_delta(path, storage_options=self._storage_options or None).collect()
        return len(df)

    def read_filtered_scan(self, table_name: str) -> int:
        """Filtered scan with predicate pushdown via Polars."""
        assert self._filter is not None
        path = self._table_path(table_name)
        date_start, date_end = self._filter.date_range
        varchar_vals = self._filter.varchar_values
        df = (
            pl.scan_delta(path, storage_options=self._storage_options or None)
            .filter(
                pl.col("date_col").is_between(
                    pl.lit(date_start).str.to_date(), pl.lit(date_end).str.to_date()
                )
                & pl.col("varchar_col").is_in(varchar_vals)
            )
            .collect()
        )
        return len(df)

    def read_aggregation(self, table_name: str) -> pa.Table:
        """Aggregation via Polars, returned as Arrow."""
        path = self._table_path(table_name)
        df = (
            pl.scan_delta(path, storage_options=self._storage_options or None)
            .group_by("varchar_col")
            .agg(
                pl.len().alias("cnt"),
                pl.col("int64_col").sum().alias("sum_val"),
                pl.col("float64_col").mean().alias("avg_val"),
                pl.col("date_col").min().alias("min_date"),
                pl.col("date_col").max().alias("max_date"),
            )
            .collect()
        )
        return df.to_arrow()

    def get_disk_usage(self, table_name: str) -> tuple[int, int]:
        """Measure disk usage of the delta table's files."""
        assert self._config is not None
        path = self._table_path(table_name)
        if self._storage_mode == "s3":
            from notebooks.benchmark.helpers.metrics import get_s3_disk_usage
            s3 = self._config.s3
            prefix = f"{s3.delta_prefix}{table_name}/"
            return get_s3_disk_usage(
                bucket=s3.bucket, prefix=prefix,
                endpoint=s3.endpoint, access_key=s3.access_key, secret_key=s3.secret_key,
            )
        from notebooks.benchmark.helpers.metrics import get_local_disk_usage
        return get_local_disk_usage(path)

    def teardown(self, table_name: str) -> None:
        """Remove the delta table files."""
        path = self._table_path(table_name)
        if self._storage_mode == "local" and os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
        elif self._storage_mode == "s3":
            # Use DuckDB to remove S3 files
            try:
                import duckdb
                assert self._config is not None
                s3 = self._config.s3
                con = duckdb.connect()
                endpoint_stripped = s3.endpoint.replace("http://", "").replace("https://", "")
                con.execute(f"""
                    CREATE OR REPLACE SECRET s3_cleanup (
                        TYPE S3, KEY_ID '{s3.access_key}', SECRET '{s3.secret_key}',
                        ENDPOINT '{endpoint_stripped}', URL_STYLE 'path', USE_SSL false
                    );
                """)
                # DuckDB doesn't have S3 delete; we'll just note it
                logger.warning(f"S3 cleanup for {path} — manual cleanup may be needed")
                con.close()
            except Exception:
                logger.warning(f"Failed to clean up S3 path: {path}")

    def close(self) -> None:
        """No persistent connections to close."""
```

---

### Task 9: Create helpers/engines/__init__.py (engine factory)

**Files:**
- Modify: `notebooks/benchmark/helpers/engines/__init__.py`

- [ ] **Step 1: Add engine factory**

```python
"""Benchmark engine implementations."""

from __future__ import annotations

from notebooks.benchmark.helpers.engines.base import BenchmarkEngine
from notebooks.benchmark.helpers.engines.delta_engine import DeltaEngine
from notebooks.benchmark.helpers.engines.ducklake_engine import DuckLakeEngine

_ENGINES: dict[str, type[BenchmarkEngine]] = {
    "ducklake": DuckLakeEngine,
    "delta": DeltaEngine,
}


def get_engine(name: str) -> BenchmarkEngine:
    """Create an engine instance by name.

    Args:
        name: Engine name ("ducklake" or "delta").

    Returns:
        Engine instance.

    Raises:
        ValueError: If engine name is not recognized.
    """
    cls = _ENGINES.get(name)
    if cls is None:
        msg = f"Unknown engine: {name}. Available: {list(_ENGINES.keys())}"
        raise ValueError(msg)
    return cls()
```

---

### Task 10: Create helpers/results.py

**Files:**
- Create: `notebooks/benchmark/helpers/results.py`

- [ ] **Step 1: Create the results aggregation module**

```python
"""Results aggregation, comparison, and export."""

from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime, timezone

import polars as pl
from loguru import logger

from notebooks.benchmark.helpers.metrics import BenchmarkResult


def results_to_dataframe(results: list[BenchmarkResult]) -> pl.DataFrame:
    """Convert list of BenchmarkResult to a Polars DataFrame.

    Args:
        results: List of benchmark results.

    Returns:
        Polars DataFrame with all results.
    """
    if not results:
        return pl.DataFrame()
    return pl.DataFrame([asdict(r) for r in results])


def pivot_comparison(df: pl.DataFrame) -> pl.DataFrame:
    """Create a side-by-side comparison of DuckLake vs Delta.

    Args:
        df: Raw results DataFrame.

    Returns:
        Pivoted DataFrame with engine metrics side by side.
    """
    if df.is_empty():
        return df
    return (
        df.select(
            "operation", "storage_mode", "table_size", "engine",
            "wall_time_seconds", "peak_memory_mb", "disk_usage_mb",
            "file_count", "throughput_rows_per_sec",
        )
        .pivot(on="engine", index=["operation", "storage_mode", "table_size"])
        .sort("operation", "storage_mode", "table_size")
    )


def speedup_ratios(df: pl.DataFrame) -> pl.DataFrame:
    """Compute speedup ratios between engines.

    Args:
        df: Raw results DataFrame.

    Returns:
        DataFrame with speedup ratios (>1 means DuckLake is faster).
    """
    if df.is_empty():
        return df

    ducklake = df.filter(pl.col("engine") == "ducklake").select(
        "operation", "storage_mode", "table_size",
        pl.col("wall_time_seconds").alias("ducklake_time"),
        pl.col("peak_memory_mb").alias("ducklake_memory"),
    )
    delta = df.filter(pl.col("engine") == "delta").select(
        "operation", "storage_mode", "table_size",
        pl.col("wall_time_seconds").alias("delta_time"),
        pl.col("peak_memory_mb").alias("delta_memory"),
    )

    joined = ducklake.join(delta, on=["operation", "storage_mode", "table_size"], how="inner")
    return joined.with_columns(
        (pl.col("delta_time") / pl.col("ducklake_time")).round(2).alias("time_speedup_ducklake_vs_delta"),
        (pl.col("delta_memory") / pl.col("ducklake_memory")).round(2).alias("memory_ratio_ducklake_vs_delta"),
    ).sort("operation", "storage_mode", "table_size")


def storage_efficiency(df: pl.DataFrame) -> pl.DataFrame:
    """Compare storage efficiency between engines.

    Args:
        df: Raw results DataFrame.

    Returns:
        DataFrame with disk usage and file count comparisons.
    """
    if df.is_empty():
        return df
    # Only write/merge operations produce meaningful disk metrics
    write_ops = df.filter(
        pl.col("operation").str.starts_with("write_") | pl.col("operation").str.starts_with("merge_")
    )
    if write_ops.is_empty():
        return write_ops
    return (
        write_ops.select(
            "operation", "storage_mode", "table_size", "engine",
            "disk_usage_mb", "file_count", "avg_file_size_mb",
        )
        .pivot(on="engine", index=["operation", "storage_mode", "table_size"])
        .sort("operation", "storage_mode", "table_size")
    )


def export_results(df: pl.DataFrame, output_dir: str) -> str:
    """Export results to CSV.

    Args:
        df: Results DataFrame.
        output_dir: Directory for output files.

    Returns:
        Path to the written CSV file.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_results_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    df.write_csv(filepath)
    logger.info(f"Results exported to {filepath}")
    return filepath
```

---

### Task 11: Finalize helpers/__init__.py

**Files:**
- Modify: `notebooks/benchmark/helpers/__init__.py`

- [ ] **Step 1: Add package re-exports**

```python
"""Benchmark helpers package."""

from notebooks.benchmark.helpers.config import BenchmarkConfig, load_config
from notebooks.benchmark.helpers.data_generator import (
    generate_data,
    generate_data_batched,
    generate_merge_data,
)
from notebooks.benchmark.helpers.engines import get_engine
from notebooks.benchmark.helpers.metrics import (
    BenchmarkResult,
    build_result,
    measure_time_and_memory,
)
from notebooks.benchmark.helpers.results import (
    export_results,
    pivot_comparison,
    results_to_dataframe,
    speedup_ratios,
    storage_efficiency,
)

__all__ = [
    "BenchmarkConfig",
    "BenchmarkResult",
    "build_result",
    "export_results",
    "generate_data",
    "generate_data_batched",
    "generate_merge_data",
    "get_engine",
    "load_config",
    "measure_time_and_memory",
    "pivot_comparison",
    "results_to_dataframe",
    "speedup_ratios",
    "storage_efficiency",
]
```

---

### Task 12: Create the marimo notebook

**Files:**
- Create: `notebooks/benchmark/benchmark_ducklake_vs_delta.py`

- [ ] **Step 1: Create the marimo orchestrator notebook**

```python
import marimo

__generated_with = "0.23.1"
app = marimo.App(width="full")


@app.cell
def _():
    """Imports and configuration."""
    import marimo as mo
    from pathlib import Path
    from loguru import logger
    import statistics

    from notebooks.benchmark.helpers import (
        load_config,
        generate_data,
        generate_data_batched,
        generate_merge_data,
        get_engine,
        measure_time_and_memory,
        build_result,
        results_to_dataframe,
        pivot_comparison,
        speedup_ratios,
        storage_efficiency,
        export_results,
        BenchmarkResult,
    )
    from notebooks.benchmark.helpers.metrics import TimingResult

    CONFIG_PATH = Path("notebooks/benchmark/config.yaml")
    RESULTS_DIR = "notebooks/benchmark/results"

    config = load_config(CONFIG_PATH)
    mo.md(f"# {config.name}\n\nLoaded config: **{len(config.table_sizes)}** sizes, **{len(config.storage_modes)}** storage modes, **{len(config.engines)}** engines, **{len(config.operations.all_operations)}** operations")
    return (
        mo, Path, logger, statistics,
        load_config, generate_data, generate_data_batched, generate_merge_data,
        get_engine, measure_time_and_memory, build_result,
        results_to_dataframe, pivot_comparison, speedup_ratios, storage_efficiency,
        export_results, BenchmarkResult, TimingResult,
        CONFIG_PATH, RESULTS_DIR, config,
    )


@app.cell
def _(mo, config):
    """Size selector — choose which table sizes to run."""
    size_options = list(config.table_sizes.keys())
    size_selector = mo.ui.multiselect(
        options=size_options,
        value=["small", "medium"],
        label="Select table sizes to benchmark",
    )
    size_selector
    return size_selector, size_options


@app.cell
def _(mo, config):
    """Storage mode selector."""
    storage_selector = mo.ui.multiselect(
        options=config.storage_modes,
        value=config.storage_modes,
        label="Select storage modes",
    )
    storage_selector
    return (storage_selector,)


@app.cell
def _(mo, config):
    """Engine selector."""
    engine_selector = mo.ui.multiselect(
        options=config.engines,
        value=config.engines,
        label="Select engines",
    )
    engine_selector
    return (engine_selector,)


@app.cell
def _(mo):
    """Run button."""
    run_button = mo.ui.run_button(label="Run Benchmark")
    run_button
    return (run_button,)


@app.cell
def _(
    mo, config, logger, statistics,
    size_selector, storage_selector, engine_selector, run_button,
    generate_data, generate_data_batched, generate_merge_data,
    get_engine, measure_time_and_memory, build_result,
    BenchmarkResult, TimingResult,
):
    """Main benchmark orchestration loop."""
    mo.stop(not run_button.value, "Click 'Run Benchmark' to start.")

    all_results: list[BenchmarkResult] = []
    # Track full_scan times for scan_efficiency_ratio
    full_scan_times: dict[tuple[str, str, str], float] = {}

    selected_sizes = {k: config.table_sizes[k] for k in size_selector.value}
    selected_storages = storage_selector.value
    selected_engines = engine_selector.value
    operations = config.operations.all_operations

    total_combos = len(selected_sizes) * len(selected_storages) * len(selected_engines) * len(operations)
    logger.info(f"Starting benchmark: {total_combos} combinations")

    for size_name, row_count in selected_sizes.items():
        logger.info(f"=== Table size: {size_name} ({row_count:,} rows) ===")

        # Generate data once per size
        if row_count <= config.batch_size:
            base_data = generate_data(config.schema, row_count, seed=config.schema.seed)
        else:
            # For large sizes, generate the first batch for merge data reference
            batches = list(generate_data_batched(
                config.schema, row_count, batch_size=config.batch_size, seed=config.schema.seed
            ))
            import pyarrow as pa
            base_data = pa.concat_tables(batches)

        merge_data = generate_merge_data(
            base_data, config.schema.id_col, config.schema,
            overlap_ratio=config.schema.merge_overlap_ratio,
            seed=config.schema.seed + 1,
        )

        for storage_mode in selected_storages:
            for engine_name in selected_engines:
                engine = get_engine(engine_name)

                for operation in operations:
                    logger.info(f"  [{engine_name}] {operation} on {storage_mode} ({size_name})")

                    timings: list[TimingResult] = []
                    disk_bytes = 0
                    file_count = 0

                    total_runs = config.warmup_runs + config.repeat_runs
                    for run_idx in range(total_runs):
                        is_warmup = run_idx < config.warmup_runs

                        try:
                            engine.setup(config, storage_mode)

                            # Pre-populate for read/merge ops
                            if operation.startswith("read_") or operation.startswith("merge_"):
                                engine.write_overwrite("bench_table", base_data)

                            with measure_time_and_memory() as timing_container:
                                if operation == "write_append":
                                    engine.write_append("bench_table", base_data)
                                elif operation == "write_overwrite":
                                    engine.write_overwrite("bench_table", base_data)
                                elif operation == "merge_upsert":
                                    engine.merge_upsert("bench_table", merge_data, config.schema.id_col)
                                elif operation == "read_full_scan":
                                    engine.read_full_scan("bench_table")
                                elif operation == "read_filtered_scan":
                                    engine.read_filtered_scan("bench_table")
                                elif operation == "read_aggregation":
                                    engine.read_aggregation("bench_table")

                            if not is_warmup:
                                timings.append(timing_container[0])
                                # Measure disk after the last timed run
                                if run_idx == total_runs - 1:
                                    disk_bytes, file_count = engine.get_disk_usage("bench_table")

                        except Exception:
                            logger.exception(f"    FAILED: {engine_name}/{operation}/{storage_mode}/{size_name}")
                        finally:
                            engine.teardown("bench_table")
                            engine.close()

                    if timings:
                        # Take median timing
                        median_time = statistics.median(t.wall_time_seconds for t in timings)
                        median_memory = statistics.median(t.peak_memory_mb for t in timings)
                        median_timing = TimingResult(median_time, median_memory)

                        # Track full_scan time for efficiency ratio
                        full_scan_time = None
                        if operation == "read_full_scan":
                            full_scan_times[(engine_name, storage_mode, size_name)] = median_time
                        elif operation == "read_filtered_scan":
                            full_scan_time = full_scan_times.get((engine_name, storage_mode, size_name))

                        result = build_result(
                            engine=engine_name,
                            operation=operation,
                            storage_mode=storage_mode,
                            table_size=size_name,
                            row_count=row_count,
                            timing=median_timing,
                            disk_bytes=disk_bytes,
                            file_count=file_count,
                            full_scan_time=full_scan_time,
                        )
                        all_results.append(result)
                        logger.info(
                            f"    OK: {median_time:.3f}s, {median_memory:.1f}MB mem, "
                            f"{disk_bytes / 1024 / 1024:.1f}MB disk, {file_count} files"
                        )

    logger.info(f"Benchmark complete: {len(all_results)} results collected")
    return (all_results, full_scan_times)


@app.cell
def _(mo, all_results, results_to_dataframe):
    """Raw results table."""
    results_df = results_to_dataframe(all_results)
    mo.md("## Raw Results")
    mo.output.replace(results_df)
    return (results_df,)


@app.cell
def _(mo, results_df, pivot_comparison):
    """Side-by-side comparison."""
    mo.md("## DuckLake vs Delta — Side by Side")
    pivot_df = pivot_comparison(results_df)
    mo.output.replace(pivot_df)
    return (pivot_df,)


@app.cell
def _(mo, results_df, speedup_ratios):
    """Speedup ratios."""
    mo.md("## Speedup Ratios (>1 means DuckLake faster)")
    speedup_df = speedup_ratios(results_df)
    mo.output.replace(speedup_df)
    return (speedup_df,)


@app.cell
def _(mo, results_df, storage_efficiency):
    """Storage efficiency comparison."""
    mo.md("## Storage Efficiency")
    storage_df = storage_efficiency(results_df)
    mo.output.replace(storage_df)
    return (storage_df,)


@app.cell
def _(mo, results_df, export_results, RESULTS_DIR):
    """Export results to CSV."""
    if not results_df.is_empty():
        csv_path = export_results(results_df, RESULTS_DIR)
        mo.md(f"Results exported to `{csv_path}`")
    return ()


if __name__ == "__main__":
    app.run()
```

---

### Task 13: Update .gitignore

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1: Add benchmark data and results directories**

Append to `.gitignore`:

```
# Benchmark data and results
notebooks/benchmark/data/
notebooks/benchmark/results/
```

---

### Task 14: Smoke test

- [ ] **Step 1: Verify all imports resolve**

Run: `uv run python -c "from notebooks.benchmark.helpers import load_config, generate_data, get_engine, measure_time_and_memory, build_result, results_to_dataframe, pivot_comparison, speedup_ratios, storage_efficiency, export_results; print('All imports OK')"`

Expected: `All imports OK`

- [ ] **Step 2: Verify config loading**

Run: `uv run python -c "
from notebooks.benchmark.helpers import load_config
cfg = load_config('notebooks/benchmark/config.yaml')
print(f'Name: {cfg.name}')
print(f'Sizes: {cfg.table_sizes}')
print(f'Operations: {cfg.operations.all_operations}')
print(f'Columns: {len(cfg.schema.columns)}')
"`

Expected:
```
Name: DuckLake v1.0 vs Delta-rs + Polars
Sizes: {'small': 100000, 'medium': 1000000, 'large': 10000000, 'xl': 100000000}
Operations: ['write_append', 'write_overwrite', 'read_full_scan', 'read_filtered_scan', 'read_aggregation', 'merge_upsert']
Columns: 16
```

- [ ] **Step 3: Verify data generation with small sample**

Run: `uv run python -c "
from notebooks.benchmark.helpers import load_config, generate_data
cfg = load_config('notebooks/benchmark/config.yaml')
t = generate_data(cfg.schema, 100)
print(f'Rows: {t.num_rows}, Cols: {t.num_columns}')
for i, name in enumerate(t.column_names):
    print(f'  {name}: {t.schema.field(i).type}')
"`

Expected: 17 columns (id + 16 data columns) with correct Arrow types.

- [ ] **Step 4: Verify engine instantiation**

Run: `uv run python -c "
from notebooks.benchmark.helpers import get_engine
dl = get_engine('ducklake')
de = get_engine('delta')
print(f'DuckLake: {dl.name}, Delta: {de.name}')
"`

Expected: `DuckLake: ducklake, Delta: delta`

- [ ] **Step 5: Verify marimo notebook parses**

Run: `uv run marimo edit notebooks/benchmark/benchmark_ducklake_vs_delta.py --headless 2>&1 | head -5`

Expected: Marimo starts without syntax errors (Ctrl+C to stop).