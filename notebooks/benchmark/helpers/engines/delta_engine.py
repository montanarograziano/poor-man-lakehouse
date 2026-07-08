"""Delta-rs + Polars benchmark engine.

Writes go straight from ``pa.RecordBatchReader`` to ``write_deltalake`` (one Delta version per
call, no per-batch commit amplification). Reads use Polars ``scan_delta`` with predicate
pushdown. Tables are partitioned by ``event_date``.
"""

from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, WriterProperties, write_deltalake
from loguru import logger

from ..data_generator import PARTITION_COL

if TYPE_CHECKING:
    from ..config import BenchmarkConfig, FilterConfig


class DeltaEngine:
    """Benchmark engine for Delta Lake using delta-rs and Polars.

    Writes via ``write_deltalake(reader, partition_by=[event_date], ...)``; reads via
    ``pl.scan_delta(...).collect()``; merges via ``DeltaTable.merge(...).execute()``.
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

    def _write(
        self,
        table_name: str,
        reader: pa.RecordBatchReader,
        schema: pa.Schema,  # noqa: ARG002 - schema is inferred from reader by delta-rs >=1.0
        mode: str,
    ) -> None:
        assert self._config is not None
        path = self._table_path(table_name)
        writer_props = WriterProperties(
            max_row_group_size=self._config.parquet_row_group_size,
            compression="SNAPPY",
        )
        write_deltalake(
            table_or_uri=path,
            data=reader,
            mode=mode,
            partition_by=[PARTITION_COL],
            target_file_size=self._config.target_file_size_bytes,
            writer_properties=writer_props,
            storage_options=self._storage_options or None,
        )

    def write_append(
        self,
        table_name: str,
        reader: pa.RecordBatchReader,
        schema: pa.Schema,
    ) -> None:
        """Append data to delta table. One Delta version per call."""
        self._write(table_name, reader, schema, mode="append")

    def write_overwrite(
        self,
        table_name: str,
        reader: pa.RecordBatchReader,
        schema: pa.Schema,
    ) -> None:
        """Overwrite delta table with new data. One Delta version per call."""
        self._write(table_name, reader, schema, mode="overwrite")

    def merge_upsert(
        self,
        table_name: str,
        source_reader: pa.RecordBatchReader,
        merge_key: str,
    ) -> None:
        """Upsert via DeltaTable.merge.

        Passes the ``RecordBatchReader`` directly: delta-rs >=1.0 accepts ``ArrowStreamExportable``
        as the source and runs the merge with ``streamed_exec=True`` (DataFusion ``LazyMemoryExec``)
        by default, pulling batches lazily and spilling to disk when the working set exceeds
        ``max_spill_size``. No source materialization here.
        """
        path = self._table_path(table_name)
        dt = DeltaTable(path, storage_options=self._storage_options or None)
        dt.merge(
            source=source_reader,
            source_alias="source",
            target_alias="target",
            predicate=f"source.{merge_key} = target.{merge_key}",
        ).when_matched_update_all().when_not_matched_insert_all().execute()

    def read_full_scan(self, table_name: str) -> int:
        """Full table scan: read every row but stream batches (no full materialization).

        ``pl.scan_delta(...).collect()`` materializes the entire result, which is
        ``~250 bytes/row × 100M rows ≈ 25 GB`` at xl scale. Use ``DeltaTable.to_pyarrow_dataset``
        and iterate the scanner to bound reader memory by one batch at a time.
        """
        path = self._table_path(table_name)
        dt = DeltaTable(path, storage_options=self._storage_options or None)
        scanner = dt.to_pyarrow_dataset().scanner()
        total = 0
        for batch in scanner.to_batches():
            total += batch.num_rows
        return total

    def read_filtered_scan(self, table_name: str) -> int:
        """Filtered scan: partition prune on ``event_date`` + value filter on ``varchar_col``.

        Streams matching rows via the pyarrow dataset scanner with predicate pushdown.
        Partition pruning narrows files; the value filter is then applied per batch.
        """
        import datetime as _dt

        import pyarrow.compute as pc
        import pyarrow.dataset as ds

        assert self._filter is not None
        path = self._table_path(table_name)
        date_start = _dt.date.fromisoformat(self._filter.date_range[0])
        date_end = _dt.date.fromisoformat(self._filter.date_range[1])
        varchar_vals = self._filter.varchar_values

        dt = DeltaTable(path, storage_options=self._storage_options or None)
        expr = (
            (ds.field(PARTITION_COL) >= date_start)
            & (ds.field(PARTITION_COL) <= date_end)
            & pc.is_in(ds.field("varchar_col"), value_set=pa.array(varchar_vals))
        )
        scanner = dt.to_pyarrow_dataset().scanner(filter=expr)
        total = 0
        for batch in scanner.to_batches():
            total += batch.num_rows
        return total

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
                pl.col(PARTITION_COL).min().alias("min_date"),
                pl.col(PARTITION_COL).max().alias("max_date"),
            )
            .collect()
        )
        return df.to_arrow()

    def get_disk_usage(self, table_name: str) -> tuple[int, int]:
        """Measure disk usage of the delta table's files."""
        assert self._config is not None
        path = self._table_path(table_name)
        if self._storage_mode == "s3":
            from ..metrics import get_s3_disk_usage

            s3 = self._config.s3
            prefix = f"{s3.delta_prefix}{table_name}/"
            return get_s3_disk_usage(
                bucket=s3.bucket,
                prefix=prefix,
                endpoint=s3.endpoint,
                access_key=s3.access_key,
                secret_key=s3.secret_key,
            )
        from ..metrics import get_local_disk_usage

        return get_local_disk_usage(path)

    def get_postgres_metadata_size(self, table_name: str) -> int:
        """Delta has no Postgres catalog — always 0."""
        return 0

    def teardown(self, table_name: str) -> None:
        """Remove the delta table files."""
        path = self._table_path(table_name)
        if self._storage_mode == "local" and os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
        elif self._storage_mode == "s3" and self._config is not None:
            from ..metrics import s3_rm_recursive

            s3 = self._config.s3
            prefix = f"{s3.delta_prefix}{table_name}/"
            try:
                n = s3_rm_recursive(
                    bucket=s3.bucket,
                    prefix=prefix,
                    endpoint=s3.endpoint,
                    access_key=s3.access_key,
                    secret_key=s3.secret_key,
                )
                logger.debug(f"Cleaned up {n} S3 objects at s3://{s3.bucket}/{prefix}")
            except Exception:
                logger.warning(f"Failed to clean up S3 path: {path}")

    def close(self) -> None:
        """No persistent connections to close."""
