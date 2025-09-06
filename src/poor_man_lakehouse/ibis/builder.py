from typing import Literal

import ibis
import polars as pl
from ibis import Table
from pyiceberg.catalog import load_catalog

from poor_man_lakehouse.config import settings
from poor_man_lakehouse.spark.builder import retrieve_current_spark_session


class IbisConnection:
    def __init__(self):
        self.connections = {
            "pyspark": ibis.pyspark.connect(session=retrieve_current_spark_session()),
            "polars": ibis.polars.connect(),
            "duckdb": ibis.duckdb.connect(database=":memory:", read_only=False),
        }
        self._catalog_config = settings.ICEBERG_STORAGE_OPTIONS | {
            "type": "rest",
            "uri": settings.NESSIE_PYICEBERG_SERVER_URI,
        }
        self.pyiceberg_catalog = load_catalog("nessie", **self._catalog_config)

    def get_connection(self, engine: Literal["pyspark", "polars", "duckdb"]):
        if engine not in self.connections:
            raise ValueError(f"Unsupported engine: {engine}")
        return self.connections[engine]

    def list_tables(self, engine: Literal["pyspark", "polars", "duckdb"]):
        con = self.get_connection(engine)
        return con.list_tables()

    def sql(self, engine: Literal["pyspark", "polars", "duckdb"], query: str):
        con = self.get_connection(engine)
        return con.sql(query)

    def read_table(
        self,
        database: str,
        table_name: str,
        engine: Literal["pyspark", "polars", "duckdb"],
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
                # Get the table from PyIceberg catalog
                iceberg_table = self.pyiceberg_catalog.load_table(
                    f"{database}.{table_name}"
                )
                lazyframe = pl.scan_iceberg(iceberg_table, reader_override="pyiceberg")
                con.create_table(f"{database}.{table_name}", lazyframe)
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
