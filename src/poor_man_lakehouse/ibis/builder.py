"""Ibis connection builder for multi-engine data access.

This module provides a unified interface for accessing data through multiple
compute engines (PySpark, Polars, DuckDB) using Ibis as the abstraction layer.

Connections are lazily initialized - engines are only started when first accessed.
"""

from functools import cached_property
from typing import Any, Literal

import ibis
import polars as pl
from ibis import Table
from loguru import logger
from pyiceberg.catalog import Catalog, load_catalog

from poor_man_lakehouse.config import settings

Engine = Literal["pyspark", "polars", "duckdb"]
SQLEngine = Literal["pyspark", "duckdb"]

# Supported engines for validation
_SUPPORTED_ENGINES: set[str] = {"pyspark", "polars", "duckdb"}
_SQL_ENGINES: set[str] = {"pyspark", "duckdb"}


class IbisConnection:
    """Multi-engine connection manager using Ibis as a unified interface.

    Provides lazy access to PySpark, Polars, and DuckDB engines through a common
    Ibis interface, with support for reading Iceberg tables via PyIceberg.

    Connections are only initialized when first accessed, avoiding expensive
    startup costs (especially for Spark) when not all engines are needed.

    Example:
        >>> conn = (
        ...     IbisConnection()
        ... )
        >>> # Spark session only starts when this is called:
        >>> spark_conn = conn.get_connection(
        ...     "pyspark"
        ... )
        >>> # DuckDB connection is independent:
        >>> duck_conn = conn.get_connection(
        ...     "duckdb"
        ... )
    """

    def __init__(self) -> None:
        """Initialize the connection manager.

        Note: Actual engine connections are lazily initialized on first access.
        """
        self._catalog_name = settings.CATALOG_NAME
        logger.debug(f"IbisConnection initialized with catalog: {self._catalog_name}")

    @cached_property
    def _pyspark_connection(self) -> Any:
        """Lazily initialize PySpark Ibis connection."""
        # Import here to avoid circular imports and delay Spark startup
        from poor_man_lakehouse.spark.builder import retrieve_current_spark_session

        logger.info("Initializing PySpark connection...")
        return ibis.pyspark.connect(session=retrieve_current_spark_session())

    @cached_property
    def _polars_connection(self) -> Any:
        """Lazily initialize Polars Ibis connection."""
        logger.debug("Initializing Polars connection...")
        return ibis.polars.connect()

    @cached_property
    def _duckdb_connection(self) -> Any:
        """Lazily initialize DuckDB Ibis connection."""
        logger.debug("Initializing DuckDB connection...")
        return ibis.duckdb.connect(database=":memory:", read_only=False)

    @cached_property
    def pyiceberg_catalog(self) -> Catalog:
        """Lazily initialize PyIceberg catalog.

        The catalog URI is determined by the CATALOG setting:
        - "nessie": Uses NESSIE_PYICEBERG_SERVER_URI
        - "lakekeeper": Uses LAKEKEEPER_SERVER_URI
        - Others: Uses CATALOG_URI as fallback
        """
        catalog_uri = self._get_catalog_uri()
        catalog_config = settings.ICEBERG_STORAGE_OPTIONS | {
            "type": "rest",
            "uri": catalog_uri,
        }
        logger.debug(f"Initializing PyIceberg catalog '{self._catalog_name}' at {catalog_uri}")
        return load_catalog(self._catalog_name, **catalog_config)

    def _get_catalog_uri(self) -> str:
        """Get the appropriate catalog URI based on settings."""
        catalog_type = settings.CATALOG.lower()
        if catalog_type == "nessie":
            return settings.NESSIE_REST_URI
        if catalog_type == "lakekeeper":
            return settings.LAKEKEEPER_SERVER_URI
        return settings.CATALOG_URI

    def get_connection(self, engine: Engine) -> Any:
        """Get the Ibis connection for the specified engine.

        Connections are lazily initialized on first access.

        Args:
            engine: The engine to get connection for ("pyspark", "polars", "duckdb").

        Returns:
            The Ibis connection for the specified engine.

        Raises:
            ValueError: If the engine is not supported.
        """
        if engine not in _SUPPORTED_ENGINES:
            raise ValueError(f"Unsupported engine: {engine}. Supported: {_SUPPORTED_ENGINES}")

        if engine == "pyspark":
            return self._pyspark_connection
        if engine == "polars":
            return self._polars_connection
        if engine == "duckdb":
            return self._duckdb_connection

        # Should never reach here due to validation above
        raise ValueError(f"Unsupported engine: {engine}")

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
        """Set the current database for the specified SQL engine.

        Args:
            database: The database name to set as current.
            engine: The SQL engine to configure ("pyspark" or "duckdb").

        Raises:
            ValueError: If engine doesn't support database switching.
        """
        if engine not in _SQL_ENGINES:
            raise ValueError(f"Engine '{engine}' does not support database switching")

        con = self.get_connection(engine)
        if engine == "duckdb":
            if database not in con.list_databases():
                con.create_database(database, force=True)
            if con.current_database != {database}:
                con.raw_sql(f"use memory.{database};")
        elif engine == "pyspark":
            con.catalog.setCurrentDatabase(database)

    def sql(self, query: str, engine: SQLEngine) -> Table:
        """Execute SQL queries using the specified engine.

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
            # DuckDB requires memory schema prefix for in-memory tables
            # TODO: Consider using parameterized queries or proper SQL building
            if "FROM memory." not in query and "from memory." not in query:
                query = query.replace("FROM ", "FROM memory.").replace("from ", "from memory.")

        con = self.get_connection(engine)
        return con.sql(query)

    def read_table(
        self,
        database: str,
        table_name: str,
        engine: Engine,
    ) -> Table:
        """Read an Iceberg table in lazy mode using the specified engine.

        Args:
            database: The database/namespace name.
            table_name: The table name.
            engine: The engine to use for reading ("pyspark", "polars").

        Returns:
            Lazy Ibis table expression.

        Raises:
            ValueError: If table cannot be read or engine not supported for this operation.
            NotImplementedError: If DuckDB is specified (use sql() instead).
        """
        if engine == "duckdb":
            raise NotImplementedError(
                "DuckDB read_table() is not supported. Use sql() method to query "
                "tables directly, or use 'polars' engine with PyIceberg integration."
            )

        con = self.get_connection(engine)

        if engine == "pyspark":
            try:
                return con.table(table_name)
            except Exception as e:
                raise ValueError(f"Could not read table {database}.{table_name} with PySpark: {e}") from e

        elif engine == "polars":
            try:
                # Load table via PyIceberg and scan with Polars
                iceberg_table = self.pyiceberg_catalog.load_table(f"{database}.{table_name}")
                lazyframe = pl.scan_iceberg(iceberg_table)

                # Register in Polars Ibis connection
                con.create_table(f"{database}.{table_name}", lazyframe, overwrite=True)

                # Also register in DuckDB for cross-engine queries
                duckdb_con = self.get_connection("duckdb")
                duckdb_con.create_database(database, force=True)
                duckdb_con.raw_sql(f"use memory.{database};")
                duckdb_con.create_table(table_name, obj=lazyframe, database=database, overwrite=True)

                return con.table(f"{database}.{table_name}")

            except Exception as e:
                raise ValueError(f"Could not read table {database}.{table_name} with Polars: {e}") from e

        raise ValueError(f"Unsupported engine for read_table: {engine}")
