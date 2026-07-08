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
    target_file_size_mb: int
    parquet_row_group_size: int
    table_sizes: dict[str, int]
    storage_modes: list[str]
    default_storage_modes: list[str]
    engines: list[str]
    operations: OperationsConfig
    schema: SchemaConfig
    filter: FilterConfig
    postgres: PostgresConfig
    s3: S3Config
    local: LocalConfig

    @property
    def target_file_size_bytes(self) -> int:
        """Target Parquet file size in bytes (engine writer parameter)."""
        return self.target_file_size_mb * 1024 * 1024


@dataclass(frozen=True)
class RunSelection:
    """Concrete subset of the config to execute in one benchmark run.

    All fields must be subsets of the corresponding ``BenchmarkConfig`` fields.
    """

    sizes: list[str]
    storage_modes: list[str]
    engines: list[str]
    operations: list[str]

    @classmethod
    def all_from(cls, cfg: BenchmarkConfig) -> RunSelection:
        """Return a selection covering everything defined in the config."""
        return cls(
            sizes=list(cfg.table_sizes.keys()),
            storage_modes=cfg.default_storage_modes or cfg.storage_modes,
            engines=list(cfg.engines),
            operations=cfg.operations.all_operations,
        )


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
        host=pg["host"],
        port=pg["port"],
        database=pg["database"],
        user=pg["user"],
        password=pg["password"],
    )

    s3_raw = raw["s3"]
    s3 = S3Config(
        endpoint=s3_raw["endpoint"],
        access_key=s3_raw["access_key"],
        secret_key=s3_raw["secret_key"],
        bucket=s3_raw["bucket"],
        ducklake_prefix=s3_raw["ducklake_prefix"],
        delta_prefix=s3_raw["delta_prefix"],
    )

    local_raw = raw["local"]
    # Resolve base_path against the YAML file's directory (not cwd) so the data location
    # is identical regardless of where load_config is called from. Without this, JupyterLab
    # (cwd = notebook dir) and the CLI (cwd = repo root) compute different absolute paths
    # and DuckLake's catalog rejects the second one as a DATA_PATH mismatch.
    raw_base = local_raw["base_path"]
    base_path_resolved = raw_base if Path(raw_base).is_absolute() else str((path.parent / raw_base).resolve())
    local = LocalConfig(
        base_path=base_path_resolved,
        ducklake_prefix=local_raw["ducklake_prefix"],
        delta_prefix=local_raw["delta_prefix"],
    )

    bench = raw["benchmark"]
    storage_modes = raw["storage_modes"]
    default_storage_modes = raw.get("default_storage_modes") or storage_modes
    return BenchmarkConfig(
        name=bench["name"],
        warmup_runs=bench["warmup_runs"],
        repeat_runs=bench["repeat_runs"],
        batch_size=bench["batch_size"],
        target_file_size_mb=bench.get("target_file_size_mb", 16),
        parquet_row_group_size=bench.get("parquet_row_group_size", 122_880),
        table_sizes=raw["table_sizes"],
        storage_modes=storage_modes,
        default_storage_modes=default_storage_modes,
        engines=raw["engines"],
        operations=operations,
        schema=schema,
        filter=filter_cfg,
        postgres=postgres,
        s3=s3,
        local=local,
    )
