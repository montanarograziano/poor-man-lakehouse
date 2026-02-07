"""Ibis connection builder for multi-engine data access via Lakekeeper catalog.

This module provides a unified interface for accessing data through multiple
compute engines (PySpark, Polars, DuckDB) using Ibis as the abstraction layer.
All engines connect to the Lakekeeper REST catalog for Iceberg table management.

- PySpark: Connects to Lakekeeper via Spark's REST catalog integration.
- DuckDB: Attaches the Lakekeeper catalog via the DuckDB Iceberg extension.
- Polars: Uses PyIceberg as an intermediary (no native Lakekeeper support).

Connections are lazily initialized - engines are only started when first accessed.
"""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Literal, overload

import ibis
import polars as pl
from ibis import Table
from loguru import logger
from pyiceberg.catalog import Catalog, load_catalog

from poor_man_lakehouse.config import settings

if TYPE_CHECKING:
    from ibis.backends.duckdb import Backend as DuckDBBackend
    from ibis.backends.polars import Backend as PolarsBackend
    from ibis.backends.pyspark import Backend as PySparkBackend

Engine = Literal["pyspark", "polars", "duckdb"]
SQLEngine = Literal["pyspark", "duckdb"]

# Supported engines for validation
_SUPPORTED_ENGINES: set[str] = {"pyspark", "polars", "duckdb"}
_SQL_ENGINES: set[str] = {"pyspark", "duckdb"}


class IbisConnection:
    """Multi-engine connection manager using Ibis with Lakekeeper catalog.

    Provides lazy access to PySpark, Polars, and DuckDB engines through a common
    Ibis interface. All SQL engines (Spark, DuckDB) connect directly to the
    Lakekeeper REST catalog. Polars uses PyIceberg as an intermediary since it
    doesn't support Lakekeeper natively.

    Raises:
        ValueError: If the configured catalog is not 'lakekeeper'.

    Example:
        >>> conn = (
        ...     IbisConnection()
        ... )
        >>> # Spark session only starts when this is called:
        >>> spark_conn = conn.get_connection(
        ...     "pyspark"
        ... )
        >>> # DuckDB connects to Lakekeeper catalog:
        >>> duck_conn = conn.get_connection(
        ...     "duckdb"
        ... )
    """

    def __init__(self) -> None:
        """Initialize the connection manager.

        Validates that the configured catalog is Lakekeeper and prepares
        connection parameters.

        Raises:
            ValueError: If settings.CATALOG is not 'lakekeeper'.
        """
        if settings.CATALOG.lower() != "lakekeeper":
            raise ValueError(
                f"Only the Lakekeeper catalog is supported by IbisConnection. "
                f"Got: '{settings.CATALOG}'. Set CATALOG=lakekeeper in your environment."
            )
        self._catalog_name = settings.CATALOG_NAME
        self._lakekeeper_endpoint = f"{settings.LAKEKEEPER_SERVER_URI}"
        logger.debug(f"IbisConnection initialized with Lakekeeper catalog at {self._lakekeeper_endpoint}")

    @cached_property
    def _pyspark_connection(self) -> PySparkBackend:
        """Lazily initialize PySpark Ibis connection with Lakekeeper catalog."""
        # Import here to avoid circular imports and delay Spark startup
        from poor_man_lakehouse.spark_connector.builder import retrieve_current_spark_session

        logger.info("Initializing PySpark connection with Lakekeeper catalog...")
        return ibis.pyspark.connect(session=retrieve_current_spark_session())

    @cached_property
    def _polars_connection(self) -> PolarsBackend:
        """Lazily initialize Polars Ibis connection.

        Polars doesn't support Lakekeeper natively, so tables are loaded
        via PyIceberg and registered in the Polars connection.
        """
        logger.debug("Initializing Polars connection...")
        return ibis.polars.connect()

    @cached_property
    def _duckdb_connection(self) -> DuckDBBackend:
        """Lazily initialize DuckDB Ibis connection with Lakekeeper catalog attached.

        Configures S3/MinIO access and attaches the Lakekeeper REST catalog
        via DuckDB's Iceberg extension.
        """
        logger.debug("Initializing DuckDB connection with Lakekeeper catalog...")
        con = ibis.duckdb.connect(database=":memory:", read_only=False, extensions=["iceberg"])

        # Configure S3/MinIO access for reading Iceberg data files
        con.raw_sql(f"""
            CREATE OR REPLACE SECRET s3_secret (
                TYPE S3,
                KEY_ID '{settings.AWS_ACCESS_KEY_ID}',
                SECRET '{settings.AWS_SECRET_ACCESS_KEY}',
                ENDPOINT '{settings.AWS_ENDPOINT_URL.replace("http://", "")}',
                URL_STYLE 'path',
                USE_SSL false
            );
        """)

        # Attach Lakekeeper REST catalog via the Iceberg extension
        con.raw_sql(f"""
            ATTACH OR REPLACE '{settings.BUCKET_NAME}' AS {self._catalog_name} (
                TYPE iceberg,
                ENDPOINT '{self._lakekeeper_endpoint}',
                TOKEN ''
            );
        """)

        logger.debug(f"DuckDB attached to Lakekeeper catalog as '{self._catalog_name}'")
        return con

    @cached_property
    def pyiceberg_catalog(self) -> Catalog:
        """Lazily initialize PyIceberg catalog connected to Lakekeeper.

        Used by the Polars engine since Polars doesn't support Lakekeeper natively.
        PyIceberg provides the bridge between Polars and the Lakekeeper catalog.
        """
        catalog_config = settings.ICEBERG_STORAGE_OPTIONS | {
            "type": "rest",
            "uri": settings.LAKEKEEPER_SERVER_URI,
        }
        logger.debug(f"Initializing PyIceberg catalog '{self._catalog_name}' at {settings.LAKEKEEPER_SERVER_URI}")
        return load_catalog(self._catalog_name, **catalog_config)

    @overload
    def get_connection(self, engine: Literal["pyspark"]) -> PySparkBackend: ...
    @overload
    def get_connection(self, engine: Literal["polars"]) -> PolarsBackend: ...
    @overload
    def get_connection(self, engine: Literal["duckdb"]) -> DuckDBBackend: ...

    def get_connection(self, engine: Engine) -> DuckDBBackend | PolarsBackend | PySparkBackend:
        """Get the Ibis connection for the specified engine.

        Connections are lazily initialized on first access.

        Args:
            engine: The engine to get connection for ("pyspark", "polars", "duckdb").

        Returns:
            The Ibis connection for the specified engine.

        Raises:
            ValueError: If the engine is not supported.
        """
        if engine == "pyspark":
            return self._pyspark_connection
        if engine == "polars":
            return self._polars_connection
        if engine == "duckdb":
            return self._duckdb_connection

        raise ValueError(f"Unsupported engine: {engine}. Supported: {_SUPPORTED_ENGINES}")

    def list_tables(self, engine: Engine) -> list[str]:
        """List all tables available in the specified engine.

        Args:
            engine: The engine to list tables from.

        Returns:
            List of table names available in the engine.
        """
        con = self.get_connection(engine)
        tables: list[str] = con.list_tables()
        return tables

    def set_current_database(self, database: str, engine: SQLEngine) -> None:
        """Set the current database/namespace for the specified SQL engine.

        Args:
            database: The database name to set as current.
            engine: The SQL engine to configure ("pyspark" or "duckdb").

        Raises:
            ValueError: If engine doesn't support database switching.
        """
        if engine not in _SQL_ENGINES:
            raise ValueError(f"Engine '{engine}' does not support database switching")

        if engine == "duckdb":
            self._duckdb_connection.raw_sql(f"USE {self._catalog_name}.{database};")
        elif engine == "pyspark":
            self._pyspark_connection.raw_sql(f"USE {database}")

    def sql(self, query: str, engine: SQLEngine) -> Table:
        """Execute SQL queries using the specified engine.

        For DuckDB, the query runs against the attached Lakekeeper catalog.
        For PySpark, the query runs against the Spark session's Lakekeeper catalog.

        Note: Polars does not support SQL execution through Ibis.
        Use read_table() for Polars access to Iceberg tables.

        Args:
            query: The SQL query string to execute.
            engine: The engine to use ("pyspark" or "duckdb").

        Returns:
            Ibis table expression with query results.

        Raises:
            ValueError: If engine doesn't support SQL execution.
        """
        if engine not in _SQL_ENGINES:
            raise ValueError(
                f"SQL execution only supports {_SQL_ENGINES} engines, got: '{engine}'. "
                f"For Polars, use read_table() instead."
            )

        if engine == "duckdb":
            self.set_current_database("default", engine)
            return self._duckdb_connection.sql(query)

        return self._pyspark_connection.sql(query)

    def read_table(
        self,
        database: str,
        table_name: str,
        engine: Engine,
    ) -> Table:
        """Read an Iceberg table in lazy mode using the specified engine.

        All engines read from the Lakekeeper catalog:
        - PySpark: Direct Spark catalog access.
        - DuckDB: Reads from the attached Lakekeeper catalog via Iceberg extension.
        - Polars: Loads via PyIceberg (no native Lakekeeper support).

        Args:
            database: The database/namespace name.
            table_name: The table name.
            engine: The engine to use for reading ("pyspark", "polars", "duckdb").

        Returns:
            Lazy Ibis table expression.

        Raises:
            ValueError: If the table cannot be read.
        """
        if engine == "pyspark":
            try:
                return self._pyspark_connection.table(table_name)
            except Exception as e:
                raise ValueError(f"Could not read table {database}.{table_name} with PySpark: {e}") from e

        elif engine == "duckdb":
            try:
                self.set_current_database(database, engine)
                return self._duckdb_connection.sql(f"SELECT * FROM {self._catalog_name}.{database}.{table_name}")  # noqa: S608
            except Exception as e:
                raise ValueError(f"Could not read table {database}.{table_name} with DuckDB: {e}") from e

        elif engine == "polars":
            try:
                # Load table via PyIceberg and scan with Polars
                iceberg_table = self.pyiceberg_catalog.load_table(f"{database}.{table_name}")
                lazyframe = pl.scan_iceberg(iceberg_table)

                # Register in Polars Ibis connection
                polars_con = self._polars_connection
                polars_con.create_table(f"{database}.{table_name}", lazyframe, overwrite=True)

                return polars_con.table(f"{database}.{table_name}")

            except Exception as e:
                raise ValueError(f"Could not read table {database}.{table_name} with Polars: {e}") from e

        raise ValueError(f"Unsupported engine for read_table: {engine}")
