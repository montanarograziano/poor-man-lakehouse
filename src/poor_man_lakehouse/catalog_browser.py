"""Catalog-agnostic browser for Iceberg table metadata.

Provides a uniform interface for browsing namespaces, tables, and schemas
across different catalog backends (Lakekeeper, Nessie) using PyIceberg's
REST catalog support.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger
from pyiceberg.catalog import load_catalog

from poor_man_lakehouse.config import settings

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

_CATALOG_URI_MAP: dict[str, str] = {
    "lakekeeper": "LAKEKEEPER_SERVER_URI",
    "nessie": "NESSIE_REST_URI",
}


class CatalogBrowser:
    """Catalog-agnostic browser for Iceberg metadata.

    Automatically resolves the REST catalog URI based on the active
    CATALOG setting. Works with any catalog that exposes a REST API.

    Example:
        >>> browser = (
        ...     CatalogBrowser()
        ... )
        >>> browser.list_namespaces()
        ['default', 'staging']
        >>> browser.list_tables(
        ...     "default"
        ... )
        ['users', 'orders']
    """

    def __init__(
        self,
        catalog_uri: str | None = None,
        catalog_name: str | None = None,
    ) -> None:
        """Initialize the catalog browser.

        Args:
            catalog_uri: REST catalog URI. Auto-resolved from CATALOG setting if not provided.
            catalog_name: Catalog name. Defaults to settings.CATALOG_NAME.
        """
        self._catalog_name = catalog_name or settings.CATALOG_NAME
        self._catalog_uri = catalog_uri or self._resolve_catalog_uri()

        catalog_config = settings.ICEBERG_STORAGE_OPTIONS | {
            "type": "rest",
            "uri": self._catalog_uri,
        }
        self._catalog: Catalog = load_catalog(self._catalog_name, **catalog_config)
        logger.debug(f"CatalogBrowser initialized: catalog='{self._catalog_name}' uri='{self._catalog_uri}'")

    def _resolve_catalog_uri(self) -> str:
        """Resolve catalog URI from settings based on active CATALOG."""
        attr_name = _CATALOG_URI_MAP.get(settings.CATALOG)
        if attr_name:
            uri: str = getattr(settings, attr_name)
            return uri
        return settings.LAKEKEEPER_SERVER_URI

    def list_namespaces(self) -> list[str]:
        """List all namespaces in the catalog.

        Returns:
            List of namespace names as strings.
        """
        raw = self._catalog.list_namespaces()
        return [ns[0] if len(ns) == 1 else ".".join(ns) for ns in raw]

    def list_tables(self, namespace: str) -> list[str]:
        """List all tables in a namespace.

        Args:
            namespace: The namespace to list tables from.

        Returns:
            List of table names.
        """
        raw = self._catalog.list_tables(namespace)
        return [tbl[1] for tbl in raw]

    def get_table_schema(self, namespace: str, table_name: str) -> list[dict]:
        """Get schema details for a table.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            List of dicts with field_id, name, type, and required.
        """
        table = self._catalog.load_table(f"{namespace}.{table_name}")
        return [
            {
                "field_id": field.field_id,
                "name": field.name,
                "type": str(field.field_type),
                "required": field.required,
            }
            for field in table.schema().fields
        ]

    def get_snapshot_count(self, namespace: str, table_name: str) -> int:
        """Get the number of snapshots for a table.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            Number of snapshots.
        """
        table = self._catalog.load_table(f"{namespace}.{table_name}")
        return len(table.metadata.snapshots or [])

    def __repr__(self) -> str:
        """String representation."""
        return f"CatalogBrowser(catalog='{self._catalog_name}', uri='{self._catalog_uri}')"
