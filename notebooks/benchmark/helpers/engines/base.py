"""Abstract engine protocol for benchmark implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import pyarrow as pa

    from ..config import BenchmarkConfig


@runtime_checkable
class BenchmarkEngine(Protocol):
    """Protocol defining the benchmark engine interface.

    Both DuckLake and Delta engines must implement all methods. Bulk data is exchanged as
    ``pa.RecordBatchReader`` for true zero-copy streaming. The schema must include a
    ``event_date`` (``pa.date32``, NOT NULL) column, used as the partition column by both
    engines so write paths exercise partitioned-Parquet emission.
    """

    name: str

    def setup(self, config: BenchmarkConfig, storage_mode: str) -> None:
        """Initialize connections, create catalog/secrets, ensure clean state."""
        ...

    def write_append(
        self,
        table_name: str,
        reader: pa.RecordBatchReader,
        schema: pa.Schema,
    ) -> None:
        """Append rows to ``table_name``, creating it if needed.

        Produces exactly one new version (Delta) / one new snapshot (DuckLake) per call,
        regardless of how many batches the reader yields.
        """
        ...

    def write_overwrite(
        self,
        table_name: str,
        reader: pa.RecordBatchReader,
        schema: pa.Schema,
    ) -> None:
        """Overwrite ``table_name`` entirely with ``reader``'s output. One version/snapshot."""
        ...

    def merge_upsert(
        self,
        table_name: str,
        source_reader: pa.RecordBatchReader,
        merge_key: str,
    ) -> None:
        """Upsert: update rows matching on ``merge_key``, insert non-matching rows."""
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

    def get_postgres_metadata_size(self, table_name: str) -> int:
        """Return bytes of PostgreSQL catalog metadata attributable to this table.

        Returns 0 for engines without a Postgres catalog. For DuckLake, this is approximated
        as ``pg_database_size(catalog_db) - empty_baseline_bytes`` captured at setup; see the
        assumptions document for the precision caveat.
        """
        ...

    def teardown(self, table_name: str) -> None:
        """Drop the table and clean up all data files."""
        ...

    def close(self) -> None:
        """Release connections and resources."""
        ...
