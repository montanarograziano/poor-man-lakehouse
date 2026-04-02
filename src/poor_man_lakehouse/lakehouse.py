"""Unified lightweight connector for Iceberg table access.

Provides catalog browsing, native scans (Polars/Arrow), DuckDB engine access,
and Ibis multi-engine wrappers — all backed by a single PyIceberg catalog.
"""

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

_SQL_ENGINES: set[str] = {"pyspark", "duckdb"}
_WRITE_MODES: set[str] = {"append", "overwrite"}


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
            con.raw_sql(f"""
                ATTACH OR REPLACE '{settings.BUCKET_NAME}' AS {catalog_name} (
                    TYPE iceberg,
                    ENDPOINT '{uri_map[self._catalog_type]}',
                    TOKEN ''
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

    def ibis_duckdb(self) -> DuckDBBackend:
        """Get the DuckDB Ibis backend with catalog attached.

        Returns:
            DuckDB Ibis backend connection.
        """
        return self.duckdb_connection

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

        catalog_name = settings.CATALOG_NAME
        fqn = f"{catalog_name}.{namespace}.{table_name}"
        con = self.duckdb_connection

        con.raw_sql(f"USE {catalog_name}.{namespace};")

        if mode == "overwrite":
            con.raw_sql(f"DELETE FROM {fqn} WHERE true")  # noqa: S608

        if query is not None:
            con.raw_sql(f"INSERT INTO {fqn} {query}")  # noqa: S608
        elif data is not None:
            con.raw_sql(f"CREATE OR REPLACE TEMP VIEW _write_staging AS {data.compile()}")  # noqa: S608
            con.raw_sql(f"INSERT INTO {fqn} SELECT * FROM _write_staging")  # noqa: S608
            con.raw_sql("DROP VIEW IF EXISTS _write_staging")

        logger.info(f"Wrote to {fqn} (mode={mode}) via DuckDB")

    def create_table(self, namespace: str, table_name: str, schema_sql: str) -> None:
        """Create an Iceberg table via DuckDB.

        Args:
            namespace: The namespace name.
            table_name: The table name.
            schema_sql: Column definitions, e.g. "id INTEGER, name VARCHAR".
        """
        catalog_name = settings.CATALOG_NAME
        fqn = f"{catalog_name}.{namespace}.{table_name}"
        self.duckdb_connection.raw_sql(f"CREATE TABLE IF NOT EXISTS {fqn} ({schema_sql})")  # noqa: S608
        logger.info(f"Created table {fqn}")

    # -- Lifecycle --

    def close(self) -> None:
        """Close all active connections and clear cached properties."""
        for prop in ("duckdb_connection",):
            if prop in self.__dict__:
                del self.__dict__[prop]
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
