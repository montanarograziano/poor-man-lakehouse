from typing import Literal

import ibis
import polars as pl
from ibis import Table
from pyiceberg.catalog import load_catalog

from poor_man_lakehouse.config import settings
from poor_man_lakehouse.spark.builder import retrieve_current_spark_session

engines = Literal["pyspark", "polars", "duckdb"]
sql_engines = Literal["pyspark", "duckdb"]


class IbisConnection:
    def __init__(self):
        self._catalog_name = settings.CATALOG_NAME
        self.connections = {
            "pyspark": ibis.pyspark.connect(session=retrieve_current_spark_session()),
            "polars": ibis.polars.connect(),
            "duckdb": ibis.duckdb.connect(database=":memory:", read_only=False),
        }
        self._catalog_config = settings.ICEBERG_STORAGE_OPTIONS | {
            "type": "rest",
            "uri": settings.NESSIE_PYICEBERG_SERVER_URI,
        }
        self.pyiceberg_catalog = load_catalog(
            self._catalog_name, **self._catalog_config
        )

    def get_connection(self, engine: engines):
        if engine not in self.connections:
            raise ValueError(f"Unsupported engine: {engine}")
        return self.connections[engine]

    def list_tables(self, engine: engines):
        con = self.get_connection(engine)
        return con.list_tables()

    def set_current_database(self, database: str, engine: sql_engines):
        con = self.get_connection(engine)
        if engine == "duckdb":
            if database not in con.list_databases():
                con.create_database(database, force=True)
            if con.current_database != {database}:
                con.raw_sql(f"use memory.{database};")
        if engine == "pyspark":
            con.catalog.setCurrentDatabase(database)

    def sql(self, query: str, engine: sql_engines):
        """
        Execute SQL queries using the specified engine.

        Args:
            query: The SQL query string to execute
            engine: The engine to use for SQL execution ("pyspark" or "duckdb")

        Returns:
            Ibis table expression with query results
        """
        if engine not in ["pyspark", "duckdb"]:
            raise ValueError(
                f"SQL interface only supports 'pyspark' and 'duckdb' engines, got: {engine}"
            )
        if engine == "duckdb":
            self.set_current_database("default", engine)
            # FIXME: Add a better way to handle this
            query = (
                query.replace("FROM ", "FROM memory.")
                if "FROM memory." not in query
                else query
            )
            query = (
                query.replace("from ", "from memory.")
                if "from memory." not in query
                else query
            )

        con = self.get_connection(engine)
        return con.sql(query)

    def read_table(
        self,
        database: str,
        table_name: str,
        engine: engines,
    ) -> Table:
        """
        Read an Iceberg table in lazy mode using the specified engine.

        Args:
            database: The database/namespace name
            table_name: The table name
            engine: The engine to use for reading ("pyspark", "polars", "duckdb")

        Returns:
            Lazy Ibis table expression
        """
        con = self.get_connection(engine)

        if engine == "pyspark":
            # For PySpark, first check if table exists in the current context
            # Since we're using Nessie catalog and the table is already in the catalog
            try:
                # Try with just the table name first (since we set the current database)
                return con.table(table_name)
            except Exception as e:
                raise ValueError(
                    f"Could not read table {database}.{table_name} with PySpark: {e}"
                )

        elif engine == "polars":
            # For Polars, we need to use PyIceberg to get the table and then create Ibis table
            try:
                duckdb_con = self.get_connection("duckdb")
                duckdb_con.create_database(database, force=True)
                # Get the table from PyIceberg catalog
                iceberg_table = self.pyiceberg_catalog.load_table(
                    f"{database}.{table_name}"
                )
                lazyframe = pl.scan_iceberg(iceberg_table, reader_override="pyiceberg")
                con.create_table(f"{database}.{table_name}", lazyframe, overwrite=True)
                duckdb_con.raw_sql(f"use memory.{database};")
                duckdb_con.create_table(
                    table_name,
                    obj=lazyframe,
                    database=database,
                    overwrite=True,
                )

                return con.table(f"{database}.{table_name}")

            except Exception as e:
                raise ValueError(
                    f"Could not read table {database}.{table_name} with Polars: {e}"
                )

        elif engine == "duckdb":
            raise NotImplementedError(
                "DuckDB read_table is not implemented yet. Use sql() method instead."
            )


conn = IbisConnection()
