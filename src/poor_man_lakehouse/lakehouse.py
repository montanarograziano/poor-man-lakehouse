"""Unified lightweight connector for Iceberg table access.

Provides catalog browsing, native scans (Polars/Arrow), DuckDB engine access,
and Ibis multi-engine wrappers — all backed by a single PyIceberg catalog.
"""

from collections.abc import Mapping
from datetime import datetime
from functools import cached_property
from typing import Literal, Self

import ibis.expr.types as ir
import polars as pl
import pyarrow as pa
from ibis.backends.duckdb import Backend as DuckDBBackend
from ibis.backends.polars import Backend as PolarsBackend
from ibis.backends.pyspark import Backend as PySparkBackend
from loguru import logger
from pyiceberg.table import Table

from poor_man_lakehouse.catalog import LakehouseCatalogType, get_catalog
from poor_man_lakehouse.config import settings

SQLEngine = Literal["pyspark", "duckdb"]
WriteMode = Literal["append", "overwrite"]
IcebergTableAspect = Literal["snapshots", "manifests", "column_stats", "partition_stats"]

_SQL_ENGINES: set[str] = {"pyspark", "duckdb"}
_WRITE_MODES: set[str] = {"append", "overwrite"}

# DuckDB iceberg extension metadata functions; all accept an attached-catalog table name.
_ASPECT_FUNCTIONS: dict[str, str] = {
    "snapshots": "iceberg_snapshots",
    "manifests": "iceberg_metadata",
    "column_stats": "iceberg_column_stats",
    "partition_stats": "iceberg_partition_stats",
}


class LakehouseConnection:
    """Unified connection manager for Iceberg table access.

    Provides catalog browsing, native Polars/Arrow scans, DuckDB engine access,
    and Ibis multi-engine wrappers. All operations go through a single PyIceberg
    catalog instance created by get_catalog().

    Supports catalogs: lakekeeper, nessie, postgres, glue.

    Example:
        >>> conn = LakehouseConnection()
        >>> conn.list_namespaces()
        ['default', 'staging']
        >>> lf = conn.scan_polars(
        ...     "default",
        ...     "users",
        ... )
        >>> duck = conn.duckdb_connection
    """

    def __init__(self, catalog_type: LakehouseCatalogType | None = None) -> None:
        """Initialize the connection.

        Args:
            catalog_type: Catalog backend to use. Defaults to settings.CATALOG.
        """
        self._catalog_type = (catalog_type or settings.CATALOG).lower()
        self.catalog = get_catalog(self._catalog_type)  # type: ignore[arg-type]
        logger.debug(f"LakehouseConnection initialized (catalog_type={self._catalog_type})")

    @staticmethod
    def _fqn(namespace: str, table_name: str) -> str:
        """Fully qualified table name in the attached DuckDB catalog."""
        return f"{settings.CATALOG_NAME}.{namespace}.{table_name}"

    # -- Catalog browsing --

    def list_namespaces(self) -> list[str]:
        """List all namespaces in the catalog."""
        raw = self.catalog.list_namespaces()
        return [ns[0] if len(ns) == 1 else ".".join(ns) for ns in raw]

    def create_namespace(self, namespace: str) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace: The namespace name to create.
        """
        self.catalog.create_namespace(namespace)
        logger.info(f"Created namespace '{namespace}'")

    def list_tables(self, namespace: str) -> list[str]:
        """List all tables in a namespace.

        Args:
            namespace: The namespace to list tables from.

        Returns:
            List of table names.
        """
        raw = self.catalog.list_tables(namespace)
        return [tbl[1] for tbl in raw]

    def load_table(self, namespace: str, table_name: str) -> Table:
        """Load an Iceberg table object.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            PyIceberg Table object with full metadata access.
        """
        return self.catalog.load_table(f"{namespace}.{table_name}")

    def table_schema(self, namespace: str, table_name: str) -> list[dict]:
        """Get the schema of an Iceberg table.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            List of dicts with field_id, name, type, and required for each column.
        """
        table = self.load_table(namespace, table_name)
        return [
            {
                "field_id": field.field_id,
                "name": field.name,
                "type": str(field.field_type),
                "required": field.required,
            }
            for field in table.schema().fields
        ]

    def snapshot_history(self, namespace: str, table_name: str) -> list[dict]:
        """Get the snapshot history of a table.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            List of snapshot dicts with snapshot_id, timestamp_ms, and summary.
        """
        table = self.load_table(namespace, table_name)
        return [
            {
                "snapshot_id": snap.snapshot_id,
                "timestamp_ms": snap.timestamp_ms,
                "summary": snap.summary.model_dump() if snap.summary else {},
            }
            for snap in (table.metadata.snapshots or [])
        ]

    # -- Native scans --

    def scan_polars(self, namespace: str, table_name: str) -> pl.LazyFrame:
        """Scan an Iceberg table and return a Polars LazyFrame.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            Polars LazyFrame for lazy evaluation.
        """
        table = self.load_table(namespace, table_name)
        return pl.scan_iceberg(table)

    def scan_arrow(self, namespace: str, table_name: str) -> pa.Table:
        """Scan an Iceberg table and return a PyArrow Table.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            PyArrow Table.
        """
        table = self.load_table(namespace, table_name)
        return table.scan().to_arrow()

    def scan_duckdb(
        self,
        namespace: str,
        table_name: str,
        *,
        snapshot_id: int | None = None,
        as_of: datetime | str | None = None,
    ) -> ir.Table:
        """Scan an Iceberg table through DuckDB, optionally time-travelling.

        Uses DuckDB's native `AT` clause on the attached catalog (iceberg
        extension). Requires an attached catalog (lakekeeper, nessie, glue).

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.
            snapshot_id: Read the table at this Iceberg snapshot id
                (`AT (VERSION => ...)`). Mutually exclusive with as_of.
            as_of: Read the table as of this timestamp (`AT (TIMESTAMP => ...)`).
                Accepts a datetime or a string DuckDB can cast to TIMESTAMP.

        Returns:
            Ibis table expression over the (possibly historical) table state.

        Raises:
            ValueError: If both snapshot_id and as_of are provided.
        """
        if snapshot_id is not None and as_of is not None:
            raise ValueError("'snapshot_id' and 'as_of' are mutually exclusive")

        at_clause = ""
        if snapshot_id is not None:
            at_clause = f" AT (VERSION => {snapshot_id})"
        elif as_of is not None:
            ts = as_of.isoformat(sep=" ") if isinstance(as_of, datetime) else as_of
            at_clause = f" AT (TIMESTAMP => TIMESTAMP '{ts}')"

        return self.duckdb_connection.sql(f"SELECT * FROM {self._fqn(namespace, table_name)}{at_clause}")  # noqa: S608

    def inspect_table(
        self,
        namespace: str,
        table_name: str,
        aspect: IcebergTableAspect = "snapshots",
    ) -> ir.Table:
        """Inspect Iceberg table metadata through DuckDB's native functions.

        Complements the PyIceberg-based snapshot_history() with the iceberg
        extension's metadata table functions, which also expose manifest-level
        detail and file statistics.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.
            aspect: What to inspect — "snapshots" (iceberg_snapshots),
                "manifests" (iceberg_metadata), "column_stats"
                (iceberg_column_stats), or "partition_stats"
                (iceberg_partition_stats).

        Returns:
            Ibis table expression with the requested metadata.

        Raises:
            ValueError: If aspect is not supported.
        """
        function = _ASPECT_FUNCTIONS.get(aspect)
        if function is None:
            raise ValueError(f"Unsupported aspect: '{aspect}'. Supported: {set(_ASPECT_FUNCTIONS)}")

        return self.duckdb_connection.sql(f"SELECT * FROM {function}({self._fqn(namespace, table_name)})")  # noqa: S608

    # -- DuckDB engine --

    @cached_property
    def duckdb_connection(self) -> DuckDBBackend:
        """Lazily initialize DuckDB Ibis connection with Iceberg catalog attached."""
        if self._catalog_type == "glue":
            return self._init_duckdb_glue()
        return self._init_duckdb_s3()

    def _init_duckdb_s3(self) -> DuckDBBackend:
        """Initialize DuckDB with S3/MinIO access and REST catalog."""
        import ibis

        logger.debug(f"Initializing DuckDB connection ({self._catalog_type} catalog)...")
        con = ibis.duckdb.connect(database=":memory:", read_only=False, extensions=["iceberg"])

        endpoint = settings.AWS_ENDPOINT_URL.replace("https://", "").replace("http://", "")
        use_ssl = "true" if settings.AWS_ENDPOINT_URL.startswith("https://") else "false"
        con.raw_sql(f"""
            CREATE OR REPLACE SECRET s3_secret (
                TYPE S3,
                KEY_ID '{settings.AWS_ACCESS_KEY_ID}',
                SECRET '{settings.AWS_SECRET_ACCESS_KEY}',
                REGION '{settings.AWS_DEFAULT_REGION}',
                ENDPOINT '{endpoint}',
                URL_STYLE 'path',
                USE_SSL {use_ssl}
            );
        """)

        catalog_name = settings.CATALOG_NAME
        if self._catalog_type in ("lakekeeper", "nessie"):
            uri_map: dict[str, str] = {
                "lakekeeper": settings.LAKEKEEPER_SERVER_URI,
                "nessie": settings.NESSIE_REST_URI,
            }
            # AUTHORIZATION_TYPE 'none' is the documented no-auth mode (TOKEN '' would
            # send an empty Bearer header). Credential vending is on by default and,
            # since duckdb-iceberg fixes #594/#792, carries MinIO endpoint + path-style
            # through; the static secret above remains as fallback (Nessie requires it).
            con.raw_sql(f"""
                ATTACH OR REPLACE '{settings.BUCKET_NAME}' AS {catalog_name} (
                    TYPE iceberg,
                    ENDPOINT '{uri_map[self._catalog_type]}',
                    AUTHORIZATION_TYPE 'none'
                );
            """)

        logger.debug(f"DuckDB initialized ({self._catalog_type} catalog)")
        return con

    def _init_duckdb_glue(self) -> DuckDBBackend:
        """Initialize DuckDB with AWS Glue Catalog."""
        import ibis

        logger.debug("Initializing DuckDB connection with Glue catalog...")
        con = ibis.duckdb.connect(database=":memory:", read_only=False, extensions=["iceberg"])

        con.raw_sql(f"""
            CREATE OR REPLACE SECRET s3_secret (
                TYPE S3,
                PROVIDER credential_chain,
                REGION '{settings.AWS_DEFAULT_REGION}'
            );
        """)

        catalog_name = settings.CATALOG_NAME
        glue_catalog_id_clause = ""
        if settings.GLUE_CATALOG_ID:
            glue_catalog_id_clause = f",\n                CATALOG_ID '{settings.GLUE_CATALOG_ID}'"
        con.raw_sql(f"""
            ATTACH OR REPLACE '{settings.BUCKET_NAME}' AS {catalog_name} (
                TYPE iceberg,
                CATALOG_TYPE glue,
                REGION '{settings.AWS_DEFAULT_REGION}'{glue_catalog_id_clause}
            );
        """)

        logger.debug(f"DuckDB attached to Glue catalog as '{catalog_name}'")
        return con

    # -- Ibis engine access --

    def ibis_polars(self, namespace: str, table_name: str) -> PolarsBackend:
        """Get a Polars Ibis backend with a table registered.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            Polars Ibis backend with the table registered.
        """
        import ibis

        lazyframe = self.scan_polars(namespace, table_name)
        con = ibis.polars.connect()
        con.create_table(f"{namespace}.{table_name}", lazyframe, overwrite=True)
        return con

    def ibis_pyspark(self) -> PySparkBackend:
        """Get the PySpark Ibis backend.

        Returns:
            PySpark Ibis backend connection.
        """
        import ibis

        from poor_man_lakehouse.spark_connector.builder import retrieve_current_spark_session

        logger.info("Initializing PySpark Ibis connection...")
        return ibis.pyspark.connect(session=retrieve_current_spark_session())

    # -- SQL & write operations --

    def sql(self, query: str, engine: SQLEngine = "duckdb") -> ir.Table:
        """Execute a SQL query using the specified engine.

        Args:
            query: The SQL query string.
            engine: The engine to use ("duckdb" or "pyspark").

        Returns:
            Ibis table expression with query results.

        Raises:
            ValueError: If engine is not supported for SQL.
        """
        if engine not in _SQL_ENGINES:
            raise ValueError(f"SQL execution only supports {_SQL_ENGINES}, got: '{engine}'")

        if engine == "duckdb":
            return self.duckdb_connection.sql(query)

        return self.ibis_pyspark().sql(query)

    def execute(self, statement: str) -> None:
        """Execute a SQL statement (no result) on the DuckDB engine.

        Use this for DML/DDL against the attached Iceberg catalog — UPDATE,
        DELETE, MERGE INTO, ALTER TABLE — which sql() cannot run: sql() returns
        an ibis expression and ibis prepends DESCRIBE for schema inference,
        which only works for queries.

        Iceberg DML is merge-on-read: UPDATE/DELETE since DuckDB 1.4.2,
        MERGE INTO and ALTER TABLE since 1.5.3.

        Args:
            statement: The SQL statement to execute.
        """
        self.duckdb_connection.raw_sql(statement)
        logger.debug(f"Executed statement via DuckDB: {statement[:80]}")

    def write_table(
        self,
        namespace: str,
        table_name: str,
        *,
        data: ir.Table | None = None,
        query: str | None = None,
        mode: WriteMode = "append",
    ) -> None:
        """Write data to an Iceberg table via DuckDB.

        Args:
            namespace: The namespace name.
            table_name: The table name.
            data: Ibis table expression to write. Mutually exclusive with query.
            query: SQL query whose results to write. Mutually exclusive with data.
            mode: Write mode — "append" or "overwrite".

        Raises:
            ValueError: If mode is invalid or neither data nor query is provided.
        """
        if mode not in _WRITE_MODES:
            raise ValueError(f"Unsupported write mode: '{mode}'. Supported: {_WRITE_MODES}")
        if data is None and query is None:
            raise ValueError("Either 'data' or 'query' must be provided")

        fqn = self._fqn(namespace, table_name)
        con = self.duckdb_connection

        con.raw_sql(f"USE {settings.CATALOG_NAME}.{namespace};")

        if mode == "overwrite":
            con.raw_sql(f"DELETE FROM {fqn} WHERE true")  # noqa: S608

        if query is not None:
            con.raw_sql(f"INSERT INTO {fqn} {query}")  # noqa: S608
        elif data is not None:
            con.raw_sql(f"CREATE OR REPLACE TEMP VIEW _write_staging AS {data.compile()}")  # noqa: S608
            con.raw_sql(f"INSERT INTO {fqn} SELECT * FROM _write_staging")  # noqa: S608
            con.raw_sql("DROP VIEW IF EXISTS _write_staging")

        logger.info(f"Wrote to {fqn} (mode={mode}) via DuckDB")

    def create_table(
        self,
        namespace: str,
        table_name: str,
        schema_sql: str,
        *,
        partition_by: str | None = None,
        table_properties: Mapping[str, str] | None = None,
    ) -> None:
        """Create an Iceberg table via DuckDB.

        Args:
            namespace: The namespace name.
            table_name: The table name.
            schema_sql: Column definitions, e.g. "id INTEGER, name VARCHAR".
            partition_by: Iceberg partition spec, e.g. "day(event_time), bucket(16, id)".
                Supports identity, year/month/day/hour, bucket, truncate transforms
                (DuckDB 1.5.1+, bucket/truncate 1.5.3+).
            table_properties: Iceberg table properties, e.g. {"format-version": "3"}
                for Iceberg v3 (deletion vectors, VARIANT, column defaults).
        """
        fqn = self._fqn(namespace, table_name)
        ddl = f"CREATE TABLE IF NOT EXISTS {fqn} ({schema_sql})"
        if partition_by:
            ddl += f" PARTITIONED BY ({partition_by})"
        if table_properties:
            props = ", ".join(f"'{key}' = '{value}'" for key, value in table_properties.items())
            ddl += f" WITH ({props})"
        self.duckdb_connection.raw_sql(ddl)
        logger.info(f"Created table {fqn}")

    def create_table_as(
        self,
        namespace: str,
        table_name: str,
        *,
        query: str | None = None,
        data: ir.Table | None = None,
    ) -> None:
        """Create an Iceberg table from a query or Ibis expression (CTAS) via DuckDB.

        Args:
            namespace: The namespace name.
            table_name: The table name.
            query: SQL query whose results populate the table. Mutually exclusive with data.
            data: Ibis table expression to materialize. Mutually exclusive with query.

        Raises:
            ValueError: Unless exactly one of query or data is provided.
        """
        if (query is None) == (data is None):
            raise ValueError("Exactly one of 'query' or 'data' must be provided")

        fqn = self._fqn(namespace, table_name)
        con = self.duckdb_connection

        if query is not None:
            con.raw_sql(f"CREATE TABLE {fqn} AS {query}")  # noqa: S608
        elif data is not None:
            con.raw_sql(f"CREATE OR REPLACE TEMP VIEW _ctas_staging AS {data.compile()}")  # noqa: S608
            con.raw_sql(f"CREATE TABLE {fqn} AS SELECT * FROM _ctas_staging")  # noqa: S608
            con.raw_sql("DROP VIEW IF EXISTS _ctas_staging")

        logger.info(f"Created table {fqn} (CTAS)")

    def drop_table(self, namespace: str, table_name: str, *, if_exists: bool = True) -> None:
        """Drop an Iceberg table via DuckDB.

        Args:
            namespace: The namespace name.
            table_name: The table name.
            if_exists: Do not error when the table does not exist.
        """
        fqn = self._fqn(namespace, table_name)
        exists_clause = "IF EXISTS " if if_exists else ""
        self.duckdb_connection.raw_sql(f"DROP TABLE {exists_clause}{fqn}")
        logger.info(f"Dropped table {fqn}")

    # -- Lifecycle --

    def close(self) -> None:
        """Close all active connections and clear cached properties."""
        self.__dict__.pop("duckdb_connection", None)
        logger.debug("LakehouseConnection closed")

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None:
        """Exit context manager and close connections."""
        self.close()

    def __repr__(self) -> str:
        """String representation."""
        return f"LakehouseConnection(catalog_type='{self._catalog_type}')"
