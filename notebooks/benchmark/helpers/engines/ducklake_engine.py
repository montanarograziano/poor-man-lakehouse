"""DuckLake benchmark engine using DuckDB + PostgreSQL metadata catalog.

Writes consume a registered ``pa.RecordBatchReader`` via ``INSERT INTO ... SELECT * FROM <reader>``
to stream batches without materializing the full dataset. Tables are partitioned by
``event_date`` so DuckLake emits one Parquet file per partition.
"""

from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
from loguru import logger

from ..data_generator import PARTITION_COL

if TYPE_CHECKING:
    from ..config import BenchmarkConfig, FilterConfig

_MIN_DUCKDB_VERSION = "1.5.2"


class DuckLakeEngine:
    """Benchmark engine for DuckLake v1.0.

    Uses DuckDB's ``ducklake`` extension with PostgreSQL as the metadata catalog. Data files
    live on local filesystem or S3/MinIO and are partitioned by ``event_date``.
    """

    name: str = "ducklake"

    def __init__(self) -> None:
        self._con: duckdb.DuckDBPyConnection | None = None
        self._config: BenchmarkConfig | None = None
        self._storage_mode: str = ""
        self._data_path: str = ""
        self._catalog_name: str = ""
        self._filter: FilterConfig | None = None
        self._pg_database: str = ""
        self._pg_baseline_bytes: int = 0

    def setup(self, config: BenchmarkConfig, storage_mode: str) -> None:
        """Initialize DuckDB connection, install extensions, attach DuckLake."""
        self._config = config
        self._storage_mode = storage_mode
        self._filter = config.filter
        # Separate catalog per storage mode to avoid DATA_PATH conflicts.
        self._catalog_name = f"bench_ducklake_{storage_mode}"

        version = duckdb.__version__
        if version < _MIN_DUCKDB_VERSION:
            msg = f"DuckDB {_MIN_DUCKDB_VERSION}+ required, got {version}"
            raise RuntimeError(msg)

        self._con = duckdb.connect()
        self._con.execute("INSTALL ducklake; INSTALL postgres;")
        self._con.execute("LOAD ducklake; LOAD postgres;")
        # Allow the writer to reorder rows for better file packing. With sorted-by-partition
        # input from StreamingGenerator, this is harmless.
        self._con.execute("PRAGMA preserve_insertion_order = false;")

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

        self._pg_database = f"{pg.database}_{storage_mode}"
        self._ensure_postgres_db(pg.host, pg.port, pg.user, pg.password, self._pg_database)

        self._con.execute(f"""
            CREATE OR REPLACE SECRET postgres_secret (
                TYPE postgres,
                HOST '{pg.host}',
                PORT {pg.port},
                DATABASE '{self._pg_database}',
                USER '{pg.user}',
                PASSWORD '{pg.password}'
            );
        """)
        # Attach the catalog DB for postgres_query (used by get_postgres_metadata_size).
        self._pg_attach_name = f"pg_meta_{storage_mode}"
        try:
            self._con.execute(f"ATTACH '' AS {self._pg_attach_name} (TYPE POSTGRES, SECRET postgres_secret);")
        except Exception as exc:  # pragma: no cover
            logger.warning(f"Could not ATTACH Postgres catalog for size measurement: {exc}")

        attach_uri = (
            f"ducklake:postgres:dbname={self._pg_database} host={pg.host} "
            f"port={pg.port} user={pg.user} password={pg.password}"
        )
        self._con.execute(f"""
            ATTACH OR REPLACE '{attach_uri}' AS {self._catalog_name}
                (DATA_PATH '{self._data_path}');
        """)
        self._con.execute(f"USE {self._catalog_name};")

        # Capture an empty-catalog baseline for postgres_metadata_mb.
        self._pg_baseline_bytes = self._query_pg_database_size()

        logger.info(
            f"DuckLake engine setup complete (storage={storage_mode}, data_path={self._data_path}, "
            f"pg_baseline={self._pg_baseline_bytes / 1024:.1f} KB)"
        )

    def _ensure_postgres_db(self, host: str, port: int, user: str, password: str, database: str) -> None:
        """Create the benchmark database in PostgreSQL if it doesn't exist."""
        import subprocess  # noqa: S404

        result = subprocess.run(
            [  # noqa: S607
                "psql",
                "-h",
                host,
                "-p",
                str(port),
                "-U",
                user,
                "-d",
                "postgres",
                "-c",
                f"CREATE DATABASE {database};",
            ],
            capture_output=True,
            text=True,
            env={**os.environ, "PGPASSWORD": password},
            check=False,
        )
        if result.returncode == 0:
            logger.info(f"Created PostgreSQL database: {database}")
        elif "already exists" in result.stderr:
            logger.debug(f"Database {database} already exists")
        else:
            logger.warning(f"Failed to create database {database}: {result.stderr.strip()}")

    def _qualified(self, table_name: str) -> str:
        return f"{self._catalog_name}.main.{table_name}"

    def _query_pg_database_size(self) -> int:
        """Return ``pg_database_size`` for the catalog DB via DuckDB's postgres_query.

        Requires ``ATTACH '' AS <name> (TYPE POSTGRES, SECRET postgres_secret)`` to have run
        in setup. Best-effort: returns 0 if the attach failed or the function is unavailable.
        """
        assert self._con is not None
        if not getattr(self, "_pg_attach_name", ""):
            return 0
        try:
            row = self._con.execute(
                f"SELECT * FROM postgres_query('{self._pg_attach_name}', 'SELECT pg_database_size(current_database())')"
            ).fetchone()
        except Exception as exc:  # pragma: no cover - best-effort
            logger.warning(f"Could not query pg_database_size: {exc}")
            return 0
        if row is None:
            return 0
        return int(row[0])

    def _create_table(self, fq: str, schema: pa.Schema) -> None:
        """Create the DuckLake table with the given Arrow schema, partitioned by event_date."""
        assert self._con is not None
        # Use DuckDB to derive the SQL types: register an empty Arrow table and CREATE AS.
        empty = pa.Table.from_pylist([], schema=schema)
        self._con.register("_arrow_empty", empty)
        try:
            self._con.execute(f"CREATE TABLE {fq} AS SELECT * FROM _arrow_empty WHERE 0=1")
        finally:
            self._con.unregister("_arrow_empty")
        # PARTITIONED BY is applied via ALTER on the empty table; subsequent INSERTs respect it.
        try:
            self._con.execute(f"ALTER TABLE {fq} SET PARTITIONED BY ({PARTITION_COL})")
        except Exception as exc:
            logger.warning(f"Could not set partitioning via ALTER (will fall back to unpartitioned): {exc}")

    def _insert_reader(self, fq: str, reader: pa.RecordBatchReader) -> None:
        """Stream a RecordBatchReader into ``fq`` via INSERT ... SELECT * FROM <registered>."""
        assert self._con is not None
        self._con.register("_arrow_src", reader)
        try:
            self._con.execute(f"INSERT INTO {fq} SELECT * FROM _arrow_src")
        finally:
            self._con.unregister("_arrow_src")

    def write_append(
        self,
        table_name: str,
        reader: pa.RecordBatchReader,
        schema: pa.Schema,
    ) -> None:
        """Append data, creating the table on first call. One DuckLake snapshot per call."""
        assert self._con is not None
        fq = self._qualified(table_name)
        try:
            self._con.execute(f"SELECT 1 FROM {fq} LIMIT 0")
        except duckdb.CatalogException:
            self._create_table(fq, schema)
        self._insert_reader(fq, reader)

    def write_overwrite(
        self,
        table_name: str,
        reader: pa.RecordBatchReader,
        schema: pa.Schema,
    ) -> None:
        """Drop and recreate the table with new data. One DuckLake snapshot per call."""
        assert self._con is not None
        fq = self._qualified(table_name)
        self._con.execute(f"DROP TABLE IF EXISTS {fq}")
        self._create_table(fq, schema)
        self._insert_reader(fq, reader)

    def merge_upsert(
        self,
        table_name: str,
        source_reader: pa.RecordBatchReader,
        merge_key: str,
    ) -> None:
        """MERGE INTO with upsert semantics. Streams source via the registered reader."""
        assert self._con is not None
        fq = self._qualified(table_name)
        self._con.register("_merge_src", source_reader)
        non_key_cols = [name for name in source_reader.schema.names if name != merge_key]
        set_clause = ", ".join(f"{c} = source.{c}" for c in non_key_cols)
        insert_cols = ", ".join(source_reader.schema.names)
        insert_vals = ", ".join(f"source.{c}" for c in source_reader.schema.names)
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
        """Full table scan: read every row but stream batches (no full materialization).

        We iterate the Arrow record batch reader and count rows. This forces the engine to
        read+decode every Parquet file and every column, but bounds reader memory by one
        batch at a time. Compare to `.fetch_arrow_table()` which materializes the entire
        result (~250 bytes/row × 100M rows = 25 GB at xl scale).
        """
        assert self._con is not None
        fq = self._qualified(table_name)
        reader = self._con.execute(f"SELECT * FROM {fq}").fetch_record_batch()
        total = 0
        for batch in reader:
            total += batch.num_rows
        return total

    def read_filtered_scan(self, table_name: str) -> int:
        """Filtered scan: partition prune on event_date + value filter on varchar_col."""
        assert self._con is not None
        assert self._filter is not None
        fq = self._qualified(table_name)
        date_start, date_end = self._filter.date_range
        varchar_vals = ", ".join(f"'{v}'" for v in self._filter.varchar_values)
        sql = f"""
            SELECT * FROM {fq}
            WHERE {PARTITION_COL} BETWEEN '{date_start}' AND '{date_end}'
              AND varchar_col IN ({varchar_vals})
        """  # noqa: S608
        reader = self._con.execute(sql).fetch_record_batch()
        total = 0
        for batch in reader:
            total += batch.num_rows
        return total

    def read_aggregation(self, table_name: str) -> pa.Table:
        """Aggregation query."""
        assert self._con is not None
        fq = self._qualified(table_name)
        sql = f"""
            SELECT varchar_col,
                   COUNT(*) AS cnt,
                   SUM(int64_col) AS sum_val,
                   AVG(float64_col) AS avg_val,
                   MIN({PARTITION_COL}) AS min_date,
                   MAX({PARTITION_COL}) AS max_date
            FROM {fq}
            GROUP BY varchar_col
        """  # noqa: S608
        return self._con.execute(sql).fetch_arrow_table()

    def get_disk_usage(self, table_name: str) -> tuple[int, int]:
        """Measure disk usage of the table's data files. Includes all files under data_path."""
        assert self._config is not None
        if self._storage_mode == "s3":
            from ..metrics import get_s3_disk_usage

            s3 = self._config.s3
            return get_s3_disk_usage(
                bucket=s3.bucket,
                prefix=s3.ducklake_prefix,
                endpoint=s3.endpoint,
                access_key=s3.access_key,
                secret_key=s3.secret_key,
            )
        from ..metrics import get_local_disk_usage

        return get_local_disk_usage(self._data_path)

    def get_postgres_metadata_size(self, table_name: str) -> int:
        """Return current pg_database_size minus the empty-catalog baseline (bytes).

        Approximation, not a tight per-table metric. See the assumptions document.
        """
        current = self._query_pg_database_size()
        delta = current - self._pg_baseline_bytes
        return max(delta, 0)

    def teardown(self, table_name: str) -> None:
        """Drop the table and clean up all data files."""
        if self._con is None:
            return
        fq = self._qualified(table_name)
        try:
            self._con.execute(f"DROP TABLE IF EXISTS {fq}")
        except Exception:
            logger.warning(f"Failed to drop table {fq}")

        if self._storage_mode == "local" and self._data_path and os.path.exists(self._data_path):
            shutil.rmtree(self._data_path, ignore_errors=True)
            os.makedirs(self._data_path, exist_ok=True)
        elif self._storage_mode == "s3" and self._config is not None:
            self._cleanup_s3_files()

    def _cleanup_s3_files(self) -> None:
        """Remove data files from S3/MinIO using botocore."""
        from ..metrics import s3_rm_recursive

        assert self._config is not None
        s3 = self._config.s3
        prefix = s3.ducklake_prefix
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
            logger.warning(f"Failed to clean up S3 path: s3://{s3.bucket}/{prefix}")

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._con is not None:
            try:
                self._con.execute("USE memory;")
                self._con.execute(f"DETACH IF EXISTS {self._catalog_name};")
                if getattr(self, "_pg_attach_name", ""):
                    self._con.execute(f"DETACH IF EXISTS {self._pg_attach_name};")
            except Exception as exc:
                logger.debug(f"close() detach warning: {exc}")
            self._con.close()
            self._con = None
