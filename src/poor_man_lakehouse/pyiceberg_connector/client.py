"""PyIceberg client for catalog-agnostic Iceberg table management.

Provides direct access to Iceberg table operations (schema evolution,
snapshots, time travel, partition inspection) without requiring Spark.
Works with any REST-compatible catalog (Lakekeeper, Nessie, etc.).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
from loguru import logger
from pyiceberg.catalog import load_catalog

from poor_man_lakehouse.config import settings

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table


class PyIcebergClient:
    """Standalone PyIceberg client for Iceberg table management.

    Provides catalog browsing, table loading, schema inspection,
    snapshot history, and Polars/Arrow scan capabilities without
    requiring a JVM or Spark session.

    Example:
        >>> client = PyIcebergClient()
        >>> namespaces = client.list_namespaces()
        >>> table = client.load_table("default", "users")
        >>> df = client.scan_to_polars("default", "users")
        >>> history = client.snapshot_history("default", "users")
    """

    def __init__(
        self,
        catalog_uri: str | None = None,
        catalog_name: str | None = None,
        storage_options: dict | None = None,
    ) -> None:
        """Initialize the PyIceberg client.

        Args:
            catalog_uri: REST catalog URI. Defaults to settings.LAKEKEEPER_SERVER_URI.
            catalog_name: Catalog name. Defaults to settings.CATALOG_NAME.
            storage_options: Iceberg storage options. Defaults to settings.ICEBERG_STORAGE_OPTIONS.
        """
        self.catalog_name = catalog_name or settings.CATALOG_NAME
        self._catalog_uri = catalog_uri or settings.LAKEKEEPER_SERVER_URI
        self._storage_options = storage_options or settings.ICEBERG_STORAGE_OPTIONS

        catalog_config = self._storage_options | {
            "type": "rest",
            "uri": self._catalog_uri,
        }
        self._catalog: Catalog = load_catalog(self.catalog_name, **catalog_config)
        logger.debug(f"PyIcebergClient initialized: catalog='{self.catalog_name}' uri='{self._catalog_uri}'")

    def list_namespaces(self) -> list[tuple[str, ...]]:
        """List all namespaces in the catalog."""
        return self._catalog.list_namespaces()

    def list_tables(self, namespace: str) -> list[tuple[str, ...]]:
        """List all tables in a namespace.

        Args:
            namespace: The namespace to list tables from.

        Returns:
            List of (namespace, table_name) tuples.
        """
        return self._catalog.list_tables(namespace)

    def load_table(self, namespace: str, table_name: str) -> Table:
        """Load an Iceberg table object.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            PyIceberg Table object with full metadata access.
        """
        return self._catalog.load_table(f"{namespace}.{table_name}")

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
                "summary": dict(snap.summary) if snap.summary else {},
            }
            for snap in (table.metadata.snapshots or [])
        ]

    def scan_to_polars(self, namespace: str, table_name: str) -> pl.LazyFrame:
        """Scan an Iceberg table and return a Polars LazyFrame.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            Polars LazyFrame for lazy evaluation.
        """
        table = self.load_table(namespace, table_name)
        return pl.scan_iceberg(table)

    def scan_to_arrow(self, namespace: str, table_name: str):
        """Scan an Iceberg table and return an Arrow table.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            PyArrow Table.
        """
        table = self.load_table(namespace, table_name)
        return table.scan().to_arrow()

    def __repr__(self) -> str:
        """String representation."""
        return f"PyIcebergClient(catalog='{self.catalog_name}', uri='{self._catalog_uri}')"
