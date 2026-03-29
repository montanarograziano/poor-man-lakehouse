# Lakehouse Optimizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add DuckDB write support, PyIceberg standalone connector, catalog browser abstraction, Polars Lakekeeper support, connection lifecycle management, connector validation, Ibis 11.0 prep, integration test infra, Spark config population, and notebook cleanup.

**Architecture:** Each feature is a self-contained module under `src/poor_man_lakehouse/` following the existing connector pattern (subpackage with `__init__.py` + implementation file). The `CatalogBrowser` abstraction sits above connectors, providing a uniform catalog browsing API backed by PyIceberg's REST catalog support. DuckDB write support extends the existing `ibis_connector`. Integration tests use `pytest-docker` with a minimal compose profile.

**Tech Stack:** Python 3.12+, PyIceberg 0.11+, DuckDB 1.5+, Polars 1.37+, Ibis 10.5+, Pydantic Settings 2.13+, pytest, Docker Compose

---

## File Map

| Action | Path | Responsibility |
|--------|------|---------------|
| Create | `src/poor_man_lakehouse/pyiceberg_connector/__init__.py` | Exports PyIcebergClient |
| Create | `src/poor_man_lakehouse/pyiceberg_connector/client.py` | Standalone PyIceberg catalog client |
| Create | `src/poor_man_lakehouse/catalog_browser.py` | Catalog-agnostic browsing abstraction |
| Modify | `src/poor_man_lakehouse/ibis_connector/builder.py` | Add DuckDB write methods + close() |
| Modify | `src/poor_man_lakehouse/polars_connector/client.py` | Add Lakekeeper support via PyIceberg |
| Modify | `src/poor_man_lakehouse/dremio_connector/builder.py` | Add close() + context manager |
| Modify | `src/poor_man_lakehouse/config.py` | Add catalog validation helper |
| Modify | `src/poor_man_lakehouse/__init__.py` | Export new classes |
| Modify | `configs/spark/spark-defaults.conf` | Populate with Iceberg/Delta/S3 defaults |
| Create | `tests/unit/test_pyiceberg_connector.py` | PyIceberg connector unit tests |
| Create | `tests/unit/test_catalog_browser.py` | Catalog browser unit tests |
| Create | `tests/integration/__init__.py` | Integration test package |
| Create | `tests/integration/conftest.py` | Docker fixtures for integration tests |
| Create | `tests/integration/test_lakekeeper_roundtrip.py` | End-to-end read/write via Lakekeeper |

---

### Task 1: DuckDB Iceberg Write Support in IbisConnection

Extend the existing `IbisConnection` so DuckDB can INSERT/UPDATE/DELETE Iceberg tables through the attached Lakekeeper catalog. Also add `close()` and context manager protocol.

**Files:**
- Modify: `src/poor_man_lakehouse/ibis_connector/builder.py`
- Test: `tests/unit/test_ibis_connector.py`

- [ ] **Step 1: Write failing tests for write_table and close**

Add to `tests/unit/test_ibis_connector.py`:

```python
class TestIbisConnectionWriteTable:
    """Tests for write_table method."""

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_write_table_raises_for_unsupported_engine(self, mock_settings):
        """Test that write_table raises for non-duckdb engines."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        with pytest.raises(ValueError, match="only supported with DuckDB"):
            conn.write_table("default", "test", "pyspark", mode="append")

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_write_table_raises_for_invalid_mode(self, mock_settings):
        """Test that write_table raises for invalid write modes."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        with pytest.raises(ValueError, match="Unsupported write mode"):
            conn.write_table("default", "test", "duckdb", mode="invalid")


class TestIbisConnectionLifecycle:
    """Tests for connection lifecycle management."""

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_context_manager_protocol(self, mock_settings):
        """Test that IbisConnection supports context manager."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        with IbisConnection() as conn:
            assert conn._catalog_name == "lakekeeper"

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_close_clears_cached_properties(self, mock_settings):
        """Test that close() invalidates cached connections."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        conn.close()
        # After close, cached properties should be cleared
        assert "_duckdb_connection" not in conn.__dict__
        assert "_pyspark_connection" not in conn.__dict__
        assert "_polars_connection" not in conn.__dict__
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_ibis_connector.py -v -k "write_table or Lifecycle"`
Expected: FAIL — `write_table` and `close` don't exist yet

- [ ] **Step 3: Implement write_table, close, and context manager**

Add to `src/poor_man_lakehouse/ibis_connector/builder.py`:

```python
WriteMode = Literal["append", "overwrite"]
_WRITE_MODES: set[str] = {"append", "overwrite"}

# Inside IbisConnection class:

def write_table(
    self,
    database: str,
    table_name: str,
    engine: Engine,
    *,
    data: Table | None = None,
    query: str | None = None,
    mode: WriteMode = "append",
) -> None:
    """Write data to an Iceberg table via DuckDB.

    DuckDB 1.5+ supports INSERT/UPDATE/DELETE on Iceberg tables through
    REST catalog integration. This is a JVM-free write path.

    Args:
        database: The database/namespace name.
        table_name: The table name.
        engine: Must be "duckdb".
        data: Ibis table expression to write. Mutually exclusive with query.
        query: SQL query whose results to write. Mutually exclusive with data.
        mode: Write mode — "append" (INSERT INTO) or "overwrite" (INSERT OVERWRITE).

    Raises:
        ValueError: If engine is not duckdb, mode is invalid, or neither data nor query provided.
    """
    if engine != "duckdb":
        raise ValueError(f"Write operations are only supported with DuckDB engine, got: '{engine}'")

    if mode not in _WRITE_MODES:
        raise ValueError(f"Unsupported write mode: '{mode}'. Supported: {_WRITE_MODES}")

    if data is None and query is None:
        raise ValueError("Either 'data' or 'query' must be provided")

    fqn = f"{self._catalog_name}.{database}.{table_name}"
    self.set_current_database(database, "duckdb")

    if mode == "overwrite":
        sql_prefix = f"INSERT OVERWRITE {fqn}"
    else:
        sql_prefix = f"INSERT INTO {fqn}"

    if query is not None:
        self._duckdb_connection.raw_sql(f"{sql_prefix} {query}")
    elif data is not None:
        # Create a temp view from the Ibis expression and insert from it
        self._duckdb_connection.raw_sql(f"CREATE OR REPLACE TEMP VIEW _write_staging AS {data.compile()}")
        self._duckdb_connection.raw_sql(f"{sql_prefix} SELECT * FROM _write_staging")
        self._duckdb_connection.raw_sql("DROP VIEW IF EXISTS _write_staging")

    logger.info(f"Wrote to {fqn} (mode={mode}) via DuckDB")

def create_table(
    self,
    database: str,
    table_name: str,
    schema_sql: str,
) -> None:
    """Create an Iceberg table via DuckDB.

    Args:
        database: The database/namespace name.
        table_name: The table name.
        schema_sql: Column definitions, e.g. "id INTEGER, name VARCHAR".
    """
    fqn = f"{self._catalog_name}.{database}.{table_name}"
    self._duckdb_connection.raw_sql(f"CREATE TABLE IF NOT EXISTS {fqn} ({schema_sql})")
    logger.info(f"Created table {fqn}")

def close(self) -> None:
    """Close all active connections and clear cached properties."""
    for prop in ("_duckdb_connection", "_pyspark_connection", "_polars_connection", "pyiceberg_catalog"):
        if prop in self.__dict__:
            del self.__dict__[prop]
    logger.debug("IbisConnection closed")

def __enter__(self) -> "IbisConnection":
    """Enter context manager."""
    return self

def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    """Exit context manager and close connections."""
    self.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_ibis_connector.py -v`
Expected: ALL PASS

- [ ] **Step 5: Run lint**

Run: `uv run ruff check src/poor_man_lakehouse/ibis_connector/ tests/unit/test_ibis_connector.py`

- [ ] **Step 6: Commit**

```bash
git add src/poor_man_lakehouse/ibis_connector/builder.py tests/unit/test_ibis_connector.py
git commit -m "feat: add DuckDB write support and connection lifecycle to IbisConnection"
```

---

### Task 2: PyIceberg Standalone Connector

Create a standalone connector for catalog-agnostic table management (schema evolution, snapshots, time travel, partition inspection) without Spark.

**Files:**
- Create: `src/poor_man_lakehouse/pyiceberg_connector/__init__.py`
- Create: `src/poor_man_lakehouse/pyiceberg_connector/client.py`
- Create: `tests/unit/test_pyiceberg_connector.py`
- Modify: `src/poor_man_lakehouse/__init__.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_pyiceberg_connector.py`:

```python
"""Unit tests for the pyiceberg_connector module."""

from unittest.mock import MagicMock, patch

import pytest


class TestPyIcebergClientInit:
    """Tests for PyIcebergClient initialization."""

    @patch("poor_man_lakehouse.pyiceberg_connector.client.settings")
    @patch("poor_man_lakehouse.pyiceberg_connector.client.load_catalog")
    def test_init_creates_catalog(self, mock_load_catalog, mock_settings):
        """Test that init creates a PyIceberg catalog."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {"s3.endpoint": "http://minio:9000"}
        mock_load_catalog.return_value = MagicMock()

        from poor_man_lakehouse.pyiceberg_connector.client import PyIcebergClient

        client = PyIcebergClient()
        mock_load_catalog.assert_called_once()
        assert client.catalog_name == "lakekeeper"

    @patch("poor_man_lakehouse.pyiceberg_connector.client.settings")
    @patch("poor_man_lakehouse.pyiceberg_connector.client.load_catalog")
    def test_init_with_custom_uri(self, mock_load_catalog, mock_settings):
        """Test init with custom catalog URI."""
        mock_settings.CATALOG_NAME = "custom"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}
        mock_load_catalog.return_value = MagicMock()

        from poor_man_lakehouse.pyiceberg_connector.client import PyIcebergClient

        client = PyIcebergClient(catalog_uri="http://custom:8181/catalog", catalog_name="my-cat")
        assert client.catalog_name == "my-cat"


class TestPyIcebergClientListOperations:
    """Tests for list operations."""

    @patch("poor_man_lakehouse.pyiceberg_connector.client.settings")
    @patch("poor_man_lakehouse.pyiceberg_connector.client.load_catalog")
    def test_list_namespaces(self, mock_load_catalog, mock_settings):
        """Test list_namespaces delegates to catalog."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}

        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = [("default",), ("staging",)]
        mock_load_catalog.return_value = mock_catalog

        from poor_man_lakehouse.pyiceberg_connector.client import PyIcebergClient

        client = PyIcebergClient()
        result = client.list_namespaces()
        assert result == [("default",), ("staging",)]

    @patch("poor_man_lakehouse.pyiceberg_connector.client.settings")
    @patch("poor_man_lakehouse.pyiceberg_connector.client.load_catalog")
    def test_list_tables(self, mock_load_catalog, mock_settings):
        """Test list_tables delegates to catalog."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}

        mock_catalog = MagicMock()
        mock_catalog.list_tables.return_value = [("default", "users"), ("default", "orders")]
        mock_load_catalog.return_value = mock_catalog

        from poor_man_lakehouse.pyiceberg_connector.client import PyIcebergClient

        client = PyIcebergClient()
        result = client.list_tables("default")
        assert result == [("default", "users"), ("default", "orders")]


class TestPyIcebergClientTableOperations:
    """Tests for table-level operations."""

    @patch("poor_man_lakehouse.pyiceberg_connector.client.settings")
    @patch("poor_man_lakehouse.pyiceberg_connector.client.load_catalog")
    def test_load_table(self, mock_load_catalog, mock_settings):
        """Test load_table returns an Iceberg table."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}

        mock_table = MagicMock()
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        from poor_man_lakehouse.pyiceberg_connector.client import PyIcebergClient

        client = PyIcebergClient()
        result = client.load_table("default", "users")
        mock_catalog.load_table.assert_called_once_with("default.users")
        assert result == mock_table

    @patch("poor_man_lakehouse.pyiceberg_connector.client.settings")
    @patch("poor_man_lakehouse.pyiceberg_connector.client.load_catalog")
    def test_scan_to_polars(self, mock_load_catalog, mock_settings):
        """Test scan_to_polars returns a LazyFrame."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}

        mock_table = MagicMock()
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        from poor_man_lakehouse.pyiceberg_connector.client import PyIcebergClient

        client = PyIcebergClient()
        # scan_to_polars calls pl.scan_iceberg internally, which needs a real table
        # so we test that load_table is called correctly
        client.load_table("default", "users")
        mock_catalog.load_table.assert_called_once_with("default.users")


class TestPyIcebergClientRepr:
    """Tests for string representation."""

    @patch("poor_man_lakehouse.pyiceberg_connector.client.settings")
    @patch("poor_man_lakehouse.pyiceberg_connector.client.load_catalog")
    def test_repr(self, mock_load_catalog, mock_settings):
        """Test __repr__ output."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}
        mock_load_catalog.return_value = MagicMock()

        from poor_man_lakehouse.pyiceberg_connector.client import PyIcebergClient

        client = PyIcebergClient()
        assert "lakekeeper" in repr(client)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_pyiceberg_connector.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Create the PyIceberg connector**

Create `src/poor_man_lakehouse/pyiceberg_connector/__init__.py`:

```python
"""PyIceberg connector for catalog-agnostic Iceberg table management."""

from poor_man_lakehouse.pyiceberg_connector.client import PyIcebergClient

__all__ = ["PyIcebergClient"]
```

Create `src/poor_man_lakehouse/pyiceberg_connector/client.py`:

```python
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

    def list_tables(self, namespace: str) -> list[tuple[str, str]]:
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

    def table_schema(self, namespace: str, table_name: str) -> dict:
        """Get the schema of an Iceberg table.

        Args:
            namespace: The namespace containing the table.
            table_name: The table name.

        Returns:
            Dict with field_id, name, type, and required for each column.
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
```

- [ ] **Step 4: Update package exports**

Add to `src/poor_man_lakehouse/__init__.py`:

```python
from poor_man_lakehouse.pyiceberg_connector import PyIcebergClient
```

And add `"PyIcebergClient"` to `__all__`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_pyiceberg_connector.py -v`
Expected: ALL PASS

- [ ] **Step 6: Run lint**

Run: `uv run ruff check src/poor_man_lakehouse/pyiceberg_connector/ tests/unit/test_pyiceberg_connector.py && uv run mypy src/`

- [ ] **Step 7: Commit**

```bash
git add src/poor_man_lakehouse/pyiceberg_connector/ tests/unit/test_pyiceberg_connector.py src/poor_man_lakehouse/__init__.py
git commit -m "feat: add standalone PyIceberg connector for catalog-agnostic table management"
```

---

### Task 3: Catalog Browser Abstraction

Create a thin `CatalogBrowser` that provides uniform catalog browsing across Nessie/Lakekeeper/Unity, backed by PyIceberg's REST catalog.

**Files:**
- Create: `src/poor_man_lakehouse/catalog_browser.py`
- Create: `tests/unit/test_catalog_browser.py`
- Modify: `src/poor_man_lakehouse/__init__.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_catalog_browser.py`:

```python
"""Unit tests for the catalog_browser module."""

from unittest.mock import MagicMock, patch

import pytest


class TestCatalogBrowserInit:
    """Tests for CatalogBrowser initialization."""

    @patch("poor_man_lakehouse.catalog_browser.load_catalog")
    @patch("poor_man_lakehouse.catalog_browser.settings")
    def test_init_uses_settings(self, mock_settings, mock_load_catalog):
        """Test that CatalogBrowser uses settings defaults."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.NESSIE_REST_URI = "http://nessie:19120/iceberg"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {"s3.endpoint": "http://minio:9000"}
        mock_load_catalog.return_value = MagicMock()

        from poor_man_lakehouse.catalog_browser import CatalogBrowser

        browser = CatalogBrowser()
        mock_load_catalog.assert_called_once()


class TestCatalogBrowserOperations:
    """Tests for catalog browsing operations."""

    @patch("poor_man_lakehouse.catalog_browser.load_catalog")
    @patch("poor_man_lakehouse.catalog_browser.settings")
    def test_list_namespaces(self, mock_settings, mock_load_catalog):
        """Test list_namespaces returns formatted list."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.NESSIE_REST_URI = "http://nessie:19120/iceberg"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}

        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = [("default",), ("staging",)]
        mock_load_catalog.return_value = mock_catalog

        from poor_man_lakehouse.catalog_browser import CatalogBrowser

        browser = CatalogBrowser()
        result = browser.list_namespaces()
        assert result == ["default", "staging"]

    @patch("poor_man_lakehouse.catalog_browser.load_catalog")
    @patch("poor_man_lakehouse.catalog_browser.settings")
    def test_list_tables(self, mock_settings, mock_load_catalog):
        """Test list_tables returns table names."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.NESSIE_REST_URI = "http://nessie:19120/iceberg"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}

        mock_catalog = MagicMock()
        mock_catalog.list_tables.return_value = [("default", "users"), ("default", "orders")]
        mock_load_catalog.return_value = mock_catalog

        from poor_man_lakehouse.catalog_browser import CatalogBrowser

        browser = CatalogBrowser()
        result = browser.list_tables("default")
        assert result == ["users", "orders"]

    @patch("poor_man_lakehouse.catalog_browser.load_catalog")
    @patch("poor_man_lakehouse.catalog_browser.settings")
    def test_get_table_schema(self, mock_settings, mock_load_catalog):
        """Test get_table_schema returns column info."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.NESSIE_REST_URI = "http://nessie:19120/iceberg"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}

        mock_field = MagicMock()
        mock_field.field_id = 1
        mock_field.name = "id"
        mock_field.field_type = "long"
        mock_field.required = True

        mock_table = MagicMock()
        mock_table.schema.return_value.fields = [mock_field]

        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        from poor_man_lakehouse.catalog_browser import CatalogBrowser

        browser = CatalogBrowser()
        result = browser.get_table_schema("default", "users")
        assert len(result) == 1
        assert result[0]["name"] == "id"


class TestCatalogBrowserCatalogUriResolution:
    """Tests for catalog URI resolution based on CATALOG setting."""

    @patch("poor_man_lakehouse.catalog_browser.load_catalog")
    @patch("poor_man_lakehouse.catalog_browser.settings")
    def test_nessie_uri_resolution(self, mock_settings, mock_load_catalog):
        """Test that nessie catalog resolves to NESSIE_REST_URI."""
        mock_settings.CATALOG = "nessie"
        mock_settings.CATALOG_NAME = "nessie"
        mock_settings.NESSIE_REST_URI = "http://nessie:19120/iceberg"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}
        mock_load_catalog.return_value = MagicMock()

        from poor_man_lakehouse.catalog_browser import CatalogBrowser

        browser = CatalogBrowser()
        assert browser._catalog_uri == "http://nessie:19120/iceberg"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_catalog_browser.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement CatalogBrowser**

Create `src/poor_man_lakehouse/catalog_browser.py`:

```python
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
        >>> browser = CatalogBrowser()
        >>> browser.list_namespaces()
        ['default', 'staging']
        >>> browser.list_tables("default")
        ['users', 'orders']
        >>> browser.get_table_schema("default", "users")
        [{'name': 'id', 'type': 'long', ...}]
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
            return getattr(settings, attr_name)
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
```

- [ ] **Step 4: Update package exports**

Add to `src/poor_man_lakehouse/__init__.py`:

```python
from poor_man_lakehouse.catalog_browser import CatalogBrowser
```

And add `"CatalogBrowser"` to `__all__`.

- [ ] **Step 5: Run tests and lint**

Run: `uv run pytest tests/unit/test_catalog_browser.py -v && uv run ruff check src/poor_man_lakehouse/catalog_browser.py && uv run mypy src/`

- [ ] **Step 6: Commit**

```bash
git add src/poor_man_lakehouse/catalog_browser.py tests/unit/test_catalog_browser.py src/poor_man_lakehouse/__init__.py
git commit -m "feat: add CatalogBrowser for catalog-agnostic metadata browsing"
```

---

### Task 4: Polars Lakekeeper Support

Extend `PolarsClient` to work with Lakekeeper (not just Unity Catalog) by using PyIceberg as the scan backend when the catalog is Lakekeeper.

**Files:**
- Modify: `src/poor_man_lakehouse/polars_connector/client.py`
- Modify: `tests/unit/test_polars_connector.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/unit/test_polars_connector.py`:

```python
class TestPolarsClientLakekeeperInit:
    """Tests for PolarsClient with Lakekeeper backend."""

    @patch("poor_man_lakehouse.polars_connector.client.settings")
    @patch("poor_man_lakehouse.polars_connector.client.load_catalog")
    def test_init_with_lakekeeper_backend(self, mock_load_catalog, mock_settings):
        """Test initialization with backend='lakekeeper'."""
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {"s3.endpoint": "http://minio:9000"}
        mock_settings.S3_STORAGE_OPTIONS = {}
        mock_load_catalog.return_value = MagicMock()

        client = PolarsClient(backend="lakekeeper")
        assert client._backend == "lakekeeper"
        mock_load_catalog.assert_called_once()

    @patch("poor_man_lakehouse.polars_connector.client.settings")
    @patch("poor_man_lakehouse.polars_connector.client.pl.Catalog")
    def test_init_defaults_to_unity(self, mock_catalog_cls, mock_settings):
        """Test that default backend is 'unity'."""
        mock_settings.UNITY_CATALOG_URI = "http://localhost:8080/"
        mock_settings.S3_STORAGE_OPTIONS = {}
        mock_catalog_cls.return_value = MagicMock()

        client = PolarsClient()
        assert client._backend == "unity"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_polars_connector.py -v -k "Lakekeeper"`
Expected: FAIL

- [ ] **Step 3: Implement dual-backend support**

Modify `src/poor_man_lakehouse/polars_connector/client.py` to add `backend` parameter to `__init__` and a PyIceberg-based scan path for Lakekeeper. The key changes:

1. Add `backend: Literal["unity", "lakekeeper"] = "unity"` parameter to `__init__`
2. When `backend="lakekeeper"`, initialize a PyIceberg catalog instead of `pl.Catalog`
3. Override `_scan_table` to use `pl.scan_iceberg()` for Lakekeeper backend
4. Override `list_catalogs`, `list_namespaces`, `list_tables` for Lakekeeper

Add at the top of the file:

```python
from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
```

Modify `__init__` to accept `backend` parameter and branch initialization:

```python
def __init__(
    self,
    unity_catalog_uri: str | None = None,
    storage_options: dict | None = None,
    require_https: bool = False,
    backend: Literal["unity", "lakekeeper"] = "unity",
) -> None:
    self._backend = backend
    self.storage_options = storage_options or settings.S3_STORAGE_OPTIONS
    self._table_cache: dict[str, pl.LazyFrame] = {}

    if backend == "lakekeeper":
        from pyiceberg.catalog import load_catalog

        self.unity_catalog_uri = unity_catalog_uri or settings.LAKEKEEPER_SERVER_URI
        catalog_config = settings.ICEBERG_STORAGE_OPTIONS | {
            "type": "rest",
            "uri": self.unity_catalog_uri,
        }
        self._pyiceberg_catalog: Catalog = load_catalog(
            settings.CATALOG_NAME, **catalog_config
        )
    else:
        self.unity_catalog_uri = unity_catalog_uri or settings.UNITY_CATALOG_URI
        self.catalog = pl.Catalog(self.unity_catalog_uri, require_https=require_https)
```

Override the list and scan methods to branch on backend. For Lakekeeper, delegate to `_pyiceberg_catalog`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_polars_connector.py -v`
Expected: ALL PASS

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check src/poor_man_lakehouse/polars_connector/ tests/unit/test_polars_connector.py
git add src/poor_man_lakehouse/polars_connector/client.py tests/unit/test_polars_connector.py
git commit -m "feat: add Lakekeeper backend support to PolarsClient via PyIceberg"
```

---

### Task 5: Connection Lifecycle for DremioConnection

Add `close()` and context manager to `DremioConnection`.

**Files:**
- Modify: `src/poor_man_lakehouse/dremio_connector/builder.py`
- Modify: `tests/unit/test_dremio_connector.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/unit/test_dremio_connector.py`:

```python
class TestDremioConnectionLifecycle:
    """Tests for connection lifecycle management."""

    def test_context_manager_protocol_exists(self):
        """Test that DremioConnection has context manager methods."""
        from poor_man_lakehouse.dremio_connector.builder import DremioConnection

        assert hasattr(DremioConnection, "__enter__")
        assert hasattr(DremioConnection, "__exit__")
        assert hasattr(DremioConnection, "close")
```

- [ ] **Step 2: Implement close and context manager**

Add to `DremioConnection` in `src/poor_man_lakehouse/dremio_connector/builder.py`:

```python
def close(self) -> None:
    """Close the Arrow Flight client."""
    if hasattr(self, "client") and self.client is not None:
        self.client.close()
        logger.debug("DremioConnection closed")

def __enter__(self) -> "DremioConnection":
    """Enter context manager."""
    return self

def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    """Exit context manager and close connection."""
    self.close()
```

- [ ] **Step 3: Run tests, lint, commit**

```bash
uv run pytest tests/unit/test_dremio_connector.py -v
git add src/poor_man_lakehouse/dremio_connector/builder.py tests/unit/test_dremio_connector.py
git commit -m "feat: add connection lifecycle management to DremioConnection"
```

---

### Task 6: Connector Catalog Validation

Add a validation helper in config that connectors can use to assert the required catalog is active.

**Files:**
- Modify: `src/poor_man_lakehouse/config.py`
- Modify: `tests/unit/test_config.py`

- [ ] **Step 1: Write failing test**

Add to `tests/unit/test_config.py`:

```python
class TestRequireCatalog:
    """Tests for require_catalog helper."""

    def test_require_catalog_passes_for_matching(self):
        """Test require_catalog passes when catalog matches."""
        from poor_man_lakehouse.config import require_catalog

        # Should not raise
        require_catalog("nessie", connector_name="test", settings=Settings(_env_file=None))

    def test_require_catalog_raises_for_mismatch(self):
        """Test require_catalog raises when catalog doesn't match."""
        from poor_man_lakehouse.config import require_catalog

        with pytest.raises(ValueError, match="requires 'lakekeeper' catalog"):
            require_catalog("lakekeeper", connector_name="IbisConnection", settings=Settings(_env_file=None))
```

- [ ] **Step 2: Implement require_catalog**

Add to `src/poor_man_lakehouse/config.py`:

```python
def require_catalog(expected: str, *, connector_name: str, settings: Settings | None = None) -> None:
    """Validate that the active catalog matches the expected one.

    Args:
        expected: The required catalog name.
        connector_name: Name of the connector for error messages.
        settings: Settings instance to check. Defaults to the global singleton.

    Raises:
        ValueError: If the active catalog doesn't match.
    """
    s = settings or get_settings()
    if s.CATALOG.lower() != expected.lower():
        raise ValueError(
            f"{connector_name} requires '{expected}' catalog, "
            f"but CATALOG='{s.CATALOG}'. Set CATALOG={expected} in your environment."
        )
```

- [ ] **Step 3: Run tests, lint, commit**

```bash
uv run pytest tests/unit/test_config.py -v -k "require_catalog"
git add src/poor_man_lakehouse/config.py tests/unit/test_config.py
git commit -m "feat: add require_catalog validation helper"
```

---

### Task 7: Populate Spark spark-defaults.conf

Fill the empty Spark config so `spark-shell` and `spark-submit` work directly on the Docker cluster.

**Files:**
- Modify: `configs/spark/spark-defaults.conf`

- [ ] **Step 1: Write the config**

Write `configs/spark/spark-defaults.conf`:

```properties
# Spark defaults for Poor Man Lakehouse Docker cluster
# These settings allow spark-shell and spark-submit to work with
# Iceberg + Delta + S3/MinIO without additional configuration.

# Iceberg + Delta extensions
spark.sql.extensions                       org.projectnessie.spark.extensions.NessieSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog            org.apache.spark.sql.delta.catalog.DeltaCatalog

# S3/MinIO configuration
spark.hadoop.fs.s3a.impl                   org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.path.style.access      true
spark.hadoop.fs.s3a.connection.ssl.enabled false

# JAR packages (resolved via Ivy on first use)
spark.jars.packages                        org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1,org.apache.hadoop:hadoop-aws:3.4.1,org.postgresql:postgresql:42.7.10,io.delta:delta-spark_2.13:4.0.1
```

- [ ] **Step 2: Commit**

```bash
git add configs/spark/spark-defaults.conf
git commit -m "build: populate spark-defaults.conf for Docker cluster"
```

---

### Task 8: Integration Test Infrastructure

Set up the integration test skeleton with Docker fixtures.

**Files:**
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/conftest.py`
- Create: `tests/integration/test_lakekeeper_roundtrip.py`

- [ ] **Step 1: Create integration test package**

Create `tests/integration/__init__.py`:

```python
"""Integration tests requiring Docker services."""
```

Create `tests/integration/conftest.py`:

```python
"""Fixtures for integration tests.

These tests require running Docker services. Start them with:
    docker compose --profile lakekeeper up -d

Skip integration tests in CI by default:
    pytest -m "not integration"
"""

import pytest

from poor_man_lakehouse.config import Settings


@pytest.fixture(scope="session")
def integration_settings():
    """Provide settings configured for integration tests.

    Expects services to be running via docker compose --profile lakekeeper.
    """
    return Settings(
        _env_file=None,
        CATALOG="lakekeeper",
        CATALOG_NAME="lakekeeper",
        AWS_ACCESS_KEY_ID="minioadmin",
        AWS_SECRET_ACCESS_KEY="miniopassword",
        AWS_ENDPOINT_URL="http://localhost:9000",
        AWS_DEFAULT_REGION="eu-central-1",
        LAKEKEEPER_SERVER_URI="http://localhost:8181/catalog",
        LAKEKEEPER_WAREHOUSE="warehouse",
        BUCKET_NAME="warehouse",
    )
```

Create `tests/integration/test_lakekeeper_roundtrip.py`:

```python
"""Integration test: create, write, and read an Iceberg table via Lakekeeper.

Requires: docker compose --profile lakekeeper up -d
Run with: uv run pytest tests/integration/ -m integration -v
"""

import pytest


@pytest.mark.integration
def test_pyiceberg_list_namespaces(integration_settings):
    """Test that PyIcebergClient can list namespaces from a running Lakekeeper."""
    from poor_man_lakehouse.pyiceberg_connector.client import PyIcebergClient

    client = PyIcebergClient(
        catalog_uri=integration_settings.LAKEKEEPER_SERVER_URI,
        catalog_name=integration_settings.CATALOG_NAME,
        storage_options=integration_settings.ICEBERG_STORAGE_OPTIONS,
    )
    namespaces = client.list_namespaces()
    assert isinstance(namespaces, list)


@pytest.mark.integration
def test_catalog_browser_list_namespaces(integration_settings):
    """Test that CatalogBrowser can list namespaces from a running Lakekeeper."""
    from poor_man_lakehouse.catalog_browser import CatalogBrowser

    browser = CatalogBrowser(
        catalog_uri=integration_settings.LAKEKEEPER_SERVER_URI,
        catalog_name=integration_settings.CATALOG_NAME,
    )
    namespaces = browser.list_namespaces()
    assert isinstance(namespaces, list)
```

- [ ] **Step 2: Run unit tests to ensure nothing broke**

Run: `uv run pytest tests/unit/ -v`
Expected: ALL PASS (integration tests are skipped by default via marker)

- [ ] **Step 3: Commit**

```bash
git add tests/integration/
git commit -m "test: add integration test infrastructure with Lakekeeper fixtures"
```

---

### Task 9: Ibis 11.0 Migration Prep

Audit for deprecated Ibis API usage and add compatibility notes.

**Files:**
- Modify: `src/poor_man_lakehouse/ibis_connector/builder.py` (if deprecated APIs found)

- [ ] **Step 1: Audit for deprecated APIs**

Search codebase for Ibis APIs deprecated in 11.0:
- `DataType.name` → use `DataType.__class__.__name__`
- `ibis.case` → use `ibis.cases()`
- `String.to_date` → use `String.as_date`
- `String.to_timestamp` → use `String.as_timestamp`
- Memtable explicit naming → use `create_table`/`create_view`

Run: `grep -rn "ibis.case\b\|\.to_date\b\|\.to_timestamp\b\|DataType\.name\b" src/ notebooks/`

- [ ] **Step 2: Fix any deprecated usage found, or document that codebase is clean**

If no deprecated APIs are found, add a comment in `ibis_connector/builder.py` noting Ibis 11.0 compatibility status.

- [ ] **Step 3: Commit if changes made**

```bash
git add src/poor_man_lakehouse/ibis_connector/
git commit -m "refactor: audit and prepare for Ibis 11.0 compatibility"
```

---

### Task 10: Notebook Cleanup

Consolidate overlapping notebooks into a clearer structure.

**Files:**
- Modify notebooks in `notebooks/` directory

- [ ] **Step 1: Audit current notebooks**

Review all 9 notebooks/scripts and identify overlaps:
- `pyspark_experiments.ipynb` — keep (Spark + catalogs)
- `ibis_experiments.ipynb` — keep (multi-engine)
- `dremio_experiments.ipynb` — keep (Dremio-specific)
- `delta_experiments.ipynb` — keep if distinct from pyspark
- `pyiceberg_experiments.ipynb` — keep, update for new PyIcebergClient
- `polars_client.ipynb` — keep, update for Lakekeeper backend
- `ducklake_experiments.ipynb` — review for DuckDB write content
- `lakekeeper_explorer.py` — keep (Marimo)
- `ducklake_example.py` — merge into ducklake_experiments or remove

- [ ] **Step 2: Update pyiceberg_experiments.ipynb to use PyIcebergClient**

Add cells showing the new `PyIcebergClient` API alongside the raw PyIceberg usage.

- [ ] **Step 3: Update polars_client.ipynb to show Lakekeeper backend**

Add cells demonstrating `PolarsClient(backend="lakekeeper")`.

- [ ] **Step 4: Commit**

```bash
git add notebooks/
git commit -m "docs: update notebooks for new PyIceberg and Polars Lakekeeper features"
```
