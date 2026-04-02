# Catalog-Agnostic Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate 6 user-facing classes into 3 by unifying the catalog layer via PyIceberg and merging overlapping connectors.

**Architecture:** A single `get_catalog()` factory in `catalog.py` replaces 4 scattered `load_catalog()` calls. A single `LakehouseConnection` class in `lakehouse.py` replaces `IbisConnection`, `PolarsClient`, `PyIcebergClient`, and `CatalogBrowser`. Spark and Dremio connectors stay with minor cleanup.

**Tech Stack:** PyIceberg (REST/SQL/Glue catalogs), Polars, DuckDB, Ibis, PySpark

**Spec:** `docs/superpowers/specs/2026-03-31-catalog-agnostic-refactor-design.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `src/poor_man_lakehouse/catalog.py` | PyIceberg catalog factory for all 4 catalog types |
| Create | `tests/unit/test_catalog.py` | Unit tests for `get_catalog()` |
| Create | `src/poor_man_lakehouse/lakehouse.py` | Unified lightweight connector |
| Create | `tests/unit/test_lakehouse.py` | Unit tests for `LakehouseConnection` |
| Modify | `src/poor_man_lakehouse/config.py` | Remove Unity settings, remove `require_catalog()` |
| Modify | `tests/unit/test_config.py` | Remove `require_catalog` tests, remove Unity references |
| Modify | `src/poor_man_lakehouse/spark_connector/builder.py` | Remove Unity builder, remove Unity from enum/packages |
| Modify | `tests/unit/test_spark_builder.py` | Remove Unity-related tests |
| Modify | `src/poor_man_lakehouse/__init__.py` | Update re-exports |
| Modify | `src/poor_man_lakehouse/spark_connector/__init__.py` | No changes needed |
| Delete | `src/poor_man_lakehouse/catalog_browser.py` | Replaced by `catalog.py` + `LakehouseConnection` |
| Delete | `src/poor_man_lakehouse/pyiceberg_connector/` | Replaced by `catalog.py` + `LakehouseConnection` |
| Delete | `src/poor_man_lakehouse/polars_connector/` | Replaced by `LakehouseConnection` |
| Delete | `src/poor_man_lakehouse/ibis_connector/` | Replaced by `LakehouseConnection` |
| Delete | `tests/unit/test_catalog_browser.py` | Old tests |
| Delete | `tests/unit/test_pyiceberg_connector.py` | Old tests |
| Delete | `tests/unit/test_polars_connector.py` | Old tests |
| Delete | `tests/unit/test_ibis_connector.py` | Old tests |

---

### Task 1: Create `catalog.py` with `get_catalog()` factory

**Files:**
- Create: `src/poor_man_lakehouse/catalog.py`
- Create: `tests/unit/test_catalog.py`

- [ ] **Step 1: Write failing tests for `get_catalog()`**

Create `tests/unit/test_catalog.py`:

```python
"""Unit tests for the catalog module."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest


class TestGetCatalogLakekeeper:
    """Tests for get_catalog with lakekeeper catalog type."""

    @patch("poor_man_lakehouse.catalog.load_catalog")
    @patch("poor_man_lakehouse.catalog.settings")
    def test_lakekeeper_creates_rest_catalog(self, mock_settings, mock_load_catalog):
        """Test that lakekeeper creates a REST catalog with correct config."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {"s3.endpoint": "http://minio:9000", "warehouse": "my-warehouse"}
        mock_load_catalog.return_value = MagicMock()

        from poor_man_lakehouse.catalog import get_catalog

        catalog = get_catalog()

        mock_load_catalog.assert_called_once_with(
            "lakekeeper",
            **{
                "s3.endpoint": "http://minio:9000",
                "warehouse": "my-warehouse",
                "type": "rest",
                "uri": "http://lakekeeper:8181/catalog",
            },
        )
        assert catalog is mock_load_catalog.return_value

    @patch("poor_man_lakehouse.catalog.load_catalog")
    @patch("poor_man_lakehouse.catalog.settings")
    def test_lakekeeper_explicit_type(self, mock_settings, mock_load_catalog):
        """Test that explicit catalog_type='lakekeeper' works."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}
        mock_load_catalog.return_value = MagicMock()

        from poor_man_lakehouse.catalog import get_catalog

        get_catalog(catalog_type="lakekeeper")
        mock_load_catalog.assert_called_once()
        _, kwargs = mock_load_catalog.call_args
        assert kwargs["type"] == "rest"
        assert kwargs["uri"] == "http://lakekeeper:8181/catalog"


class TestGetCatalogNessie:
    """Tests for get_catalog with nessie catalog type."""

    @patch("poor_man_lakehouse.catalog.load_catalog")
    @patch("poor_man_lakehouse.catalog.settings")
    def test_nessie_creates_rest_catalog(self, mock_settings, mock_load_catalog):
        """Test that nessie creates a REST catalog with NESSIE_REST_URI."""
        mock_settings.CATALOG = "nessie"
        mock_settings.CATALOG_NAME = "nessie"
        mock_settings.NESSIE_REST_URI = "http://nessie:19120/iceberg"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {"s3.endpoint": "http://minio:9000"}
        mock_load_catalog.return_value = MagicMock()

        from poor_man_lakehouse.catalog import get_catalog

        get_catalog()
        mock_load_catalog.assert_called_once()
        _, kwargs = mock_load_catalog.call_args
        assert kwargs["type"] == "rest"
        assert kwargs["uri"] == "http://nessie:19120/iceberg"


class TestGetCatalogPostgres:
    """Tests for get_catalog with postgres catalog type."""

    @patch("poor_man_lakehouse.catalog.load_catalog")
    @patch("poor_man_lakehouse.catalog.settings")
    def test_postgres_creates_sql_catalog(self, mock_settings, mock_load_catalog):
        """Test that postgres creates a SQL catalog with JDBC URI."""
        mock_settings.CATALOG = "postgres"
        mock_settings.CATALOG_NAME = "postgres"
        mock_settings.POSTGRES_HOST = "localhost"
        mock_settings.POSTGRES_DB = "lakehouse_db"
        mock_settings.POSTGRES_USER = "postgres"
        mock_settings.POSTGRES_PASSWORD = "password"  # noqa: S105
        mock_settings.ICEBERG_STORAGE_OPTIONS = {"s3.endpoint": "http://minio:9000", "warehouse": "s3://warehouse/"}
        mock_load_catalog.return_value = MagicMock()

        from poor_man_lakehouse.catalog import get_catalog

        get_catalog()
        mock_load_catalog.assert_called_once()
        _, kwargs = mock_load_catalog.call_args
        assert kwargs["type"] == "sql"
        assert kwargs["uri"] == "postgresql+psycopg://postgres:password@localhost/lakehouse_db"
        assert kwargs["warehouse"] == "s3://warehouse/"


class TestGetCatalogGlue:
    """Tests for get_catalog with glue catalog type."""

    @patch("poor_man_lakehouse.catalog.load_catalog")
    @patch("poor_man_lakehouse.catalog.settings")
    def test_glue_creates_glue_catalog(self, mock_settings, mock_load_catalog):
        """Test that glue creates a Glue catalog with region."""
        mock_settings.CATALOG = "glue"
        mock_settings.CATALOG_NAME = "glue"
        mock_settings.AWS_DEFAULT_REGION = "us-east-1"
        mock_settings.WAREHOUSE_BUCKET = "s3://my-data-lake/"
        mock_settings.GLUE_CATALOG_ID = ""
        mock_load_catalog.return_value = MagicMock()

        from poor_man_lakehouse.catalog import get_catalog

        get_catalog()
        mock_load_catalog.assert_called_once()
        _, kwargs = mock_load_catalog.call_args
        assert kwargs["type"] == "glue"
        assert kwargs["s3.region"] == "us-east-1"
        assert "glue.id" not in kwargs

    @patch("poor_man_lakehouse.catalog.load_catalog")
    @patch("poor_man_lakehouse.catalog.settings")
    def test_glue_includes_catalog_id_when_set(self, mock_settings, mock_load_catalog):
        """Test that glue.id is included when GLUE_CATALOG_ID is set."""
        mock_settings.CATALOG = "glue"
        mock_settings.CATALOG_NAME = "glue"
        mock_settings.AWS_DEFAULT_REGION = "us-east-1"
        mock_settings.WAREHOUSE_BUCKET = "s3://my-data-lake/"
        mock_settings.GLUE_CATALOG_ID = "123456789012"
        mock_load_catalog.return_value = MagicMock()

        from poor_man_lakehouse.catalog import get_catalog

        get_catalog()
        _, kwargs = mock_load_catalog.call_args
        assert kwargs["glue.id"] == "123456789012"


class TestGetCatalogValidation:
    """Tests for get_catalog input validation."""

    @patch("poor_man_lakehouse.catalog.settings")
    def test_raises_for_unsupported_catalog(self, mock_settings):
        """Test that unsupported catalog type raises ValueError."""
        mock_settings.CATALOG = "unity_catalog"

        from poor_man_lakehouse.catalog import get_catalog

        with pytest.raises(ValueError, match="Unsupported catalog type"):
            get_catalog()

    @patch("poor_man_lakehouse.catalog.settings")
    def test_raises_for_invalid_explicit_type(self, mock_settings):
        """Test that invalid explicit catalog_type raises ValueError."""
        from poor_man_lakehouse.catalog import get_catalog

        with pytest.raises(ValueError, match="Unsupported catalog type"):
            get_catalog(catalog_type="invalid")  # type: ignore[arg-type]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_catalog.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'poor_man_lakehouse.catalog'`

- [ ] **Step 3: Implement `catalog.py`**

Create `src/poor_man_lakehouse/catalog.py`:

```python
"""Catalog-agnostic PyIceberg catalog factory.

Provides a single factory function to create PyIceberg Catalog instances
for any supported backend (Lakekeeper, Nessie, PostgreSQL, AWS Glue).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, get_args

from loguru import logger
from pyiceberg.catalog import load_catalog

from poor_man_lakehouse.config import settings

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

LakehouseCatalogType = Literal["nessie", "lakekeeper", "postgres", "glue"]

_VALID_CATALOG_TYPES: set[str] = set(get_args(LakehouseCatalogType))


def get_catalog(
    catalog_type: LakehouseCatalogType | None = None,
) -> Catalog:
    """Create a PyIceberg Catalog for the specified or configured catalog type.

    Args:
        catalog_type: Catalog backend to use. Defaults to settings.CATALOG.

    Returns:
        A configured PyIceberg Catalog instance.

    Raises:
        ValueError: If the catalog type is not supported.
    """
    resolved_type = catalog_type or settings.CATALOG
    if resolved_type not in _VALID_CATALOG_TYPES:
        raise ValueError(
            f"Unsupported catalog type: '{resolved_type}'. "
            f"Supported: {sorted(_VALID_CATALOG_TYPES)}"
        )

    catalog_name = settings.CATALOG_NAME

    if resolved_type == "glue":
        config = _build_glue_config()
    elif resolved_type == "postgres":
        config = _build_postgres_config()
    else:
        config = _build_rest_config(resolved_type)

    logger.debug(f"Creating PyIceberg catalog '{catalog_name}' (type={resolved_type})")
    return load_catalog(catalog_name, **config)


def _build_rest_config(catalog_type: str) -> dict[str, str]:
    """Build config for REST catalogs (lakekeeper, nessie)."""
    uri_map: dict[str, str] = {
        "lakekeeper": settings.LAKEKEEPER_SERVER_URI,
        "nessie": settings.NESSIE_REST_URI,
    }
    return settings.ICEBERG_STORAGE_OPTIONS | {
        "type": "rest",
        "uri": uri_map[catalog_type],
    }


def _build_postgres_config() -> dict[str, str]:
    """Build config for PostgreSQL SQL catalog."""
    uri = (
        f"postgresql+psycopg://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
        f"@{settings.POSTGRES_HOST}/{settings.POSTGRES_DB}"
    )
    return settings.ICEBERG_STORAGE_OPTIONS | {
        "type": "sql",
        "uri": uri,
    }


def _build_glue_config() -> dict[str, str]:
    """Build config for AWS Glue catalog."""
    config: dict[str, str] = {
        "type": "glue",
        "s3.region": settings.AWS_DEFAULT_REGION,
        "warehouse": settings.WAREHOUSE_BUCKET.replace("s3a://", "s3://"),
    }
    if settings.GLUE_CATALOG_ID:
        config["glue.id"] = settings.GLUE_CATALOG_ID
    return config
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_catalog.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Run linting**

Run: `uv run ruff format src/poor_man_lakehouse/catalog.py tests/unit/test_catalog.py && uv run ruff check src/poor_man_lakehouse/catalog.py tests/unit/test_catalog.py`
Expected: No issues

- [ ] **Step 6: Commit**

```bash
git add src/poor_man_lakehouse/catalog.py tests/unit/test_catalog.py
git commit -m "feat: add catalog-agnostic PyIceberg catalog factory"
```

---

### Task 2: Create `lakehouse.py` — catalog browsing and native scans

**Files:**
- Create: `src/poor_man_lakehouse/lakehouse.py`
- Create: `tests/unit/test_lakehouse.py`

This task creates the core `LakehouseConnection` class with catalog browsing and native scan methods. Ibis and DuckDB engine methods are added in Task 3.

- [ ] **Step 1: Write failing tests for catalog browsing and scan methods**

Create `tests/unit/test_lakehouse.py`:

```python
"""Unit tests for the lakehouse module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestLakehouseConnectionInit:
    """Tests for LakehouseConnection initialization."""

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_init_creates_catalog(self, mock_get_catalog):
        """Test that init calls get_catalog."""
        mock_get_catalog.return_value = MagicMock()

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        mock_get_catalog.assert_called_once_with(None)
        assert conn.catalog is mock_get_catalog.return_value

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_init_with_explicit_catalog_type(self, mock_get_catalog):
        """Test that explicit catalog_type is passed through."""
        mock_get_catalog.return_value = MagicMock()

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        LakehouseConnection(catalog_type="glue")
        mock_get_catalog.assert_called_once_with("glue")


class TestLakehouseConnectionBrowsing:
    """Tests for catalog browsing methods."""

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_list_namespaces(self, mock_get_catalog):
        """Test list_namespaces returns formatted namespace names."""
        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = [("default",), ("staging",)]
        mock_get_catalog.return_value = mock_catalog

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        result = conn.list_namespaces()
        assert result == ["default", "staging"]

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_list_namespaces_multi_level(self, mock_get_catalog):
        """Test list_namespaces handles multi-level namespaces."""
        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = [("db", "schema")]
        mock_get_catalog.return_value = mock_catalog

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        result = conn.list_namespaces()
        assert result == ["db.schema"]

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_list_tables(self, mock_get_catalog):
        """Test list_tables returns table names."""
        mock_catalog = MagicMock()
        mock_catalog.list_tables.return_value = [("default", "users"), ("default", "orders")]
        mock_get_catalog.return_value = mock_catalog

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        result = conn.list_tables("default")
        assert result == ["users", "orders"]
        mock_catalog.list_tables.assert_called_once_with("default")

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_table_schema(self, mock_get_catalog):
        """Test table_schema returns field info."""
        mock_field = MagicMock()
        mock_field.field_id = 1
        mock_field.name = "id"
        mock_field.field_type = "long"
        mock_field.required = True
        mock_table = MagicMock()
        mock_table.schema.return_value.fields = [mock_field]
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_get_catalog.return_value = mock_catalog

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        result = conn.table_schema("default", "users")
        assert len(result) == 1
        assert result[0] == {"field_id": 1, "name": "id", "type": "long", "required": True}
        mock_catalog.load_table.assert_called_once_with("default.users")

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_snapshot_history(self, mock_get_catalog):
        """Test snapshot_history returns snapshot info."""
        mock_snap = MagicMock()
        mock_snap.snapshot_id = 123
        mock_snap.timestamp_ms = 1000
        mock_snap.summary.model_dump.return_value = {"operation": "append"}
        mock_table = MagicMock()
        mock_table.metadata.snapshots = [mock_snap]
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_get_catalog.return_value = mock_catalog

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        result = conn.snapshot_history("default", "users")
        assert len(result) == 1
        assert result[0]["snapshot_id"] == 123

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_snapshot_history_no_snapshots(self, mock_get_catalog):
        """Test snapshot_history handles tables with no snapshots."""
        mock_table = MagicMock()
        mock_table.metadata.snapshots = None
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_get_catalog.return_value = mock_catalog

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        result = conn.snapshot_history("default", "users")
        assert result == []


class TestLakehouseConnectionScans:
    """Tests for native scan methods."""

    @patch("poor_man_lakehouse.lakehouse.pl")
    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_scan_polars(self, mock_get_catalog, mock_pl):
        """Test scan_polars returns a Polars LazyFrame."""
        mock_table = MagicMock()
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_get_catalog.return_value = mock_catalog
        mock_lazyframe = MagicMock()
        mock_pl.scan_iceberg.return_value = mock_lazyframe

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        result = conn.scan_polars("default", "users")
        mock_catalog.load_table.assert_called_once_with("default.users")
        mock_pl.scan_iceberg.assert_called_once_with(mock_table)
        assert result is mock_lazyframe

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_scan_arrow(self, mock_get_catalog):
        """Test scan_arrow returns a PyArrow Table."""
        mock_arrow_table = MagicMock()
        mock_table = MagicMock()
        mock_table.scan.return_value.to_arrow.return_value = mock_arrow_table
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_get_catalog.return_value = mock_catalog

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        result = conn.scan_arrow("default", "users")
        assert result is mock_arrow_table

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_load_table(self, mock_get_catalog):
        """Test load_table returns a PyIceberg Table object."""
        mock_table = MagicMock()
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_get_catalog.return_value = mock_catalog

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        result = conn.load_table("default", "users")
        assert result is mock_table


class TestLakehouseConnectionLifecycle:
    """Tests for connection lifecycle management."""

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_context_manager(self, mock_get_catalog):
        """Test LakehouseConnection works as a context manager."""
        mock_get_catalog.return_value = MagicMock()

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        with LakehouseConnection() as conn:
            assert conn.catalog is not None

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_close_clears_cached_properties(self, mock_get_catalog):
        """Test close() removes cached engine properties."""
        mock_get_catalog.return_value = MagicMock()

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        # Inject a fake cached property
        conn.__dict__["_duckdb_connection"] = MagicMock()
        conn.close()
        assert "_duckdb_connection" not in conn.__dict__

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_repr(self, mock_get_catalog):
        """Test repr includes catalog type."""
        mock_get_catalog.return_value = MagicMock()

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection(catalog_type="lakekeeper")
        assert "lakekeeper" in repr(conn)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_lakehouse.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'poor_man_lakehouse.lakehouse'`

- [ ] **Step 3: Implement `lakehouse.py` — browsing, scans, and lifecycle**

Create `src/poor_man_lakehouse/lakehouse.py`:

```python
"""Unified lightweight connector for Iceberg table access.

Provides catalog browsing, native scans (Polars/Arrow), DuckDB engine access,
and Ibis multi-engine wrappers — all backed by a single PyIceberg catalog.
"""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Literal

import polars as pl
from loguru import logger

from poor_man_lakehouse.catalog import LakehouseCatalogType, get_catalog
from poor_man_lakehouse.config import settings

if TYPE_CHECKING:
    import pyarrow as pa
    from ibis.backends.duckdb import Backend as DuckDBBackend
    from ibis.backends.polars import Backend as PolarsBackend
    from ibis.backends.pyspark import Backend as PySparkBackend
    from pyiceberg.table import Table

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
        >>> lf = conn.scan_polars("default", "users")
        >>> duck = conn.duckdb_connection
    """

    def __init__(self, catalog_type: LakehouseCatalogType | None = None) -> None:
        """Initialize the connection.

        Args:
            catalog_type: Catalog backend to use. Defaults to settings.CATALOG.
        """
        self._catalog_type = catalog_type or settings.CATALOG
        self.catalog = get_catalog(catalog_type)
        logger.debug(f"LakehouseConnection initialized (catalog_type={self._catalog_type})")

    # ── Catalog browsing ──────────────────────────────────────────────

    def list_namespaces(self) -> list[str]:
        """List all namespaces in the catalog."""
        raw = self.catalog.list_namespaces()
        return [ns[0] if len(ns) == 1 else ".".join(ns) for ns in raw]

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

    # ── Native scans ──────────────────────────────────────────────────

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

    # ── DuckDB engine ─────────────────────────────────────────────────

    @cached_property
    def duckdb_connection(self) -> DuckDBBackend:
        """Lazily initialize DuckDB Ibis connection with Iceberg catalog attached.

        For REST catalogs (lakekeeper/nessie): configures S3/MinIO and attaches REST catalog.
        For Glue: uses AWS credential chain and attaches Glue catalog.
        For Postgres: configures S3/MinIO and attaches REST-less SQL catalog via DuckDB Iceberg.
        """
        import ibis

        if self._catalog_type == "glue":
            return self._init_duckdb_glue(ibis)
        return self._init_duckdb_s3(ibis)

    def _init_duckdb_s3(self, ibis: object) -> DuckDBBackend:
        """Initialize DuckDB with S3/MinIO access and REST or SQL catalog."""
        import ibis as ibis_mod

        logger.debug(f"Initializing DuckDB connection ({self._catalog_type} catalog)...")
        con = ibis_mod.duckdb.connect(database=":memory:", read_only=False, extensions=["iceberg"])

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
        # postgres catalog: DuckDB doesn't support attaching SQL catalogs directly,
        # so users read tables via scan_polars/scan_arrow or use the raw DuckDB connection

        logger.debug(f"DuckDB initialized ({self._catalog_type} catalog)")
        return con

    def _init_duckdb_glue(self, ibis: object) -> DuckDBBackend:
        """Initialize DuckDB with AWS Glue Catalog."""
        import ibis as ibis_mod

        logger.debug("Initializing DuckDB connection with Glue catalog...")
        con = ibis_mod.duckdb.connect(database=":memory:", read_only=False, extensions=["iceberg"])

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

    # ── Ibis engine access ────────────────────────────────────────────

    def ibis_duckdb(self) -> DuckDBBackend:
        """Get the DuckDB Ibis backend with catalog attached.

        Returns:
            DuckDB Ibis backend connection.
        """
        return self.duckdb_connection

    def ibis_polars(self, namespace: str, table_name: str) -> PolarsBackend:
        """Get a Polars Ibis backend with a table registered.

        Polars has no native catalog support, so the table is loaded via
        PyIceberg and registered in the Polars Ibis connection.

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

    # ── SQL & write operations ────────────────────────────────────────

    def sql(self, query: str, engine: SQLEngine = "duckdb") -> object:
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
        data: object | None = None,
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
        sql_prefix = f"INSERT OVERWRITE {fqn}" if mode == "overwrite" else f"INSERT INTO {fqn}"  # noqa: S608

        if query is not None:
            con.raw_sql(f"{sql_prefix} {query}")  # noqa: S608
        elif data is not None:
            con.raw_sql(f"CREATE OR REPLACE TEMP VIEW _write_staging AS {data.compile()}")  # noqa: S608
            con.raw_sql(f"{sql_prefix} SELECT * FROM _write_staging")  # noqa: S608
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

    # ── Lifecycle ─────────────────────────────────────────────────────

    def close(self) -> None:
        """Close all active connections and clear cached properties."""
        for prop in ("duckdb_connection",):
            if prop in self.__dict__:
                del self.__dict__[prop]
        logger.debug("LakehouseConnection closed")

    def __enter__(self) -> LakehouseConnection:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None:
        """Exit context manager and close connections."""
        self.close()

    def __repr__(self) -> str:
        """String representation."""
        return f"LakehouseConnection(catalog_type='{self._catalog_type}')"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_lakehouse.py -v`
Expected: All 14 tests PASS

- [ ] **Step 5: Run linting**

Run: `uv run ruff format src/poor_man_lakehouse/lakehouse.py tests/unit/test_lakehouse.py && uv run ruff check src/poor_man_lakehouse/lakehouse.py tests/unit/test_lakehouse.py`
Expected: No issues

- [ ] **Step 6: Commit**

```bash
git add src/poor_man_lakehouse/lakehouse.py tests/unit/test_lakehouse.py
git commit -m "feat: add LakehouseConnection unified lightweight connector"
```

---

### Task 3: Add `LakehouseConnection` DuckDB, SQL, and write tests

**Files:**
- Modify: `tests/unit/test_lakehouse.py`

- [ ] **Step 1: Add tests for SQL, write, and DuckDB engine methods**

Append to `tests/unit/test_lakehouse.py`:

```python
class TestLakehouseConnectionSQL:
    """Tests for SQL execution."""

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_sql_raises_for_unsupported_engine(self, mock_get_catalog):
        """Test sql() raises for unsupported engines."""
        mock_get_catalog.return_value = MagicMock()

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        with pytest.raises(ValueError, match="SQL execution only supports"):
            conn.sql("SELECT 1", "polars")  # type: ignore[arg-type]


class TestLakehouseConnectionWrite:
    """Tests for write operations."""

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_write_table_raises_for_invalid_mode(self, mock_get_catalog):
        """Test write_table raises for unsupported write modes."""
        mock_get_catalog.return_value = MagicMock()

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        with pytest.raises(ValueError, match="Unsupported write mode"):
            conn.write_table("default", "test", mode="invalid")  # type: ignore[arg-type]

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_write_table_raises_when_no_data_or_query(self, mock_get_catalog):
        """Test write_table raises when neither data nor query provided."""
        mock_get_catalog.return_value = MagicMock()

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        with pytest.raises(ValueError, match="Either 'data' or 'query' must be provided"):
            conn.write_table("default", "test")
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_lakehouse.py -v`
Expected: All 17 tests PASS

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_lakehouse.py
git commit -m "test: add SQL and write operation tests for LakehouseConnection"
```

---

### Task 4: Update `config.py` — remove Unity settings and `require_catalog()`

**Files:**
- Modify: `src/poor_man_lakehouse/config.py:57,70-71,204-220`
- Modify: `tests/unit/test_config.py:161-179`

- [ ] **Step 1: Update `config.py`**

Remove the `UNITY_CATALOG_URI` setting (line 71):

```python
# Delete this line:
    UNITY_CATALOG_URI: str = "http://unity_catalog:8080/"
```

Update the `CATALOG` comment (line 57):

```python
# Change from:
    CATALOG: str = "nessie"  # Options: "unity_catalog", "nessie", "lakekeeper", "postgres", "glue"
# To:
    CATALOG: str = "nessie"  # Options: "nessie", "lakekeeper", "postgres", "glue"
```

Remove the `require_catalog()` function (lines 204-220):

```python
# Delete the entire require_catalog function
```

- [ ] **Step 2: Update `tests/unit/test_config.py`**

Remove `TestRequireCatalog` class (lines 161-179):

```python
# Delete the entire TestRequireCatalog class
```

Also remove the import of `require_catalog` if present — but looking at the test file, it imports inline, so just deleting the class is sufficient.

- [ ] **Step 3: Run tests**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: All remaining tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/poor_man_lakehouse/config.py tests/unit/test_config.py
git commit -m "refactor: remove Unity Catalog settings and require_catalog from config"
```

---

### Task 5: Update `spark_connector/builder.py` — remove Unity builder

**Files:**
- Modify: `src/poor_man_lakehouse/spark_connector/builder.py:31,36,201-258,411-414`
- Modify: `tests/unit/test_spark_builder.py:10,25,27,54-57,187-198`

- [ ] **Step 1: Update `builder.py`**

Remove Unity Catalog package from `COMMON_PACKAGES` (line 31):

```python
# Delete this line from COMMON_PACKAGES:
    f"io.unitycatalog:unitycatalog-spark_{SCALA_VERSION}:0.4.0",
```

Remove `UNITY_CATALOG` from `CatalogType` enum (line 36):

```python
# Delete this line:
    UNITY_CATALOG = "unity_catalog"
```

Delete the entire `DeltaUnityCatalogSparkBuilder` class (lines 201-258).

Remove Unity from `_CATALOG_BUILDERS` dict (line 411):

```python
# Delete this line:
    CatalogType.UNITY_CATALOG: DeltaUnityCatalogSparkBuilder,
```

- [ ] **Step 2: Update `tests/unit/test_spark_builder.py`**

Remove the `DeltaUnityCatalogSparkBuilder` import (line 10):

```python
# Delete from the import list:
    DeltaUnityCatalogSparkBuilder,
```

Remove Unity-related test assertions and tests:

In `TestCatalogType.test_catalog_type_values`, delete:
```python
        assert CatalogType.UNITY_CATALOG.value == "unity_catalog"
```

Delete the entire `TestGetSparkBuilder.test_returns_unity_catalog_builder` test method.

Delete the entire `TestDeltaUnityCatalogSparkBuilder` test class (lines 187-198).

- [ ] **Step 3: Run tests**

Run: `uv run pytest tests/unit/test_spark_builder.py -v`
Expected: All remaining tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/poor_man_lakehouse/spark_connector/builder.py tests/unit/test_spark_builder.py
git commit -m "refactor: remove Unity Catalog support from spark connector"
```

---

### Task 6: Update `__init__.py` and delete old modules

**Files:**
- Modify: `src/poor_man_lakehouse/__init__.py`
- Delete: `src/poor_man_lakehouse/catalog_browser.py`
- Delete: `src/poor_man_lakehouse/pyiceberg_connector/` (entire directory)
- Delete: `src/poor_man_lakehouse/polars_connector/` (entire directory)
- Delete: `src/poor_man_lakehouse/ibis_connector/` (entire directory)
- Delete: `tests/unit/test_catalog_browser.py`
- Delete: `tests/unit/test_pyiceberg_connector.py`
- Delete: `tests/unit/test_polars_connector.py`
- Delete: `tests/unit/test_ibis_connector.py`

- [ ] **Step 1: Update `__init__.py`**

Replace the contents of `src/poor_man_lakehouse/__init__.py`:

```python
"""Poor Man Lakehouse - Multi-engine data lakehouse connectors."""

from poor_man_lakehouse.catalog import LakehouseCatalogType, get_catalog
from poor_man_lakehouse.config import Settings, get_settings, reload_settings, settings
from poor_man_lakehouse.dremio_connector import DremioConnection
from poor_man_lakehouse.lakehouse import LakehouseConnection
from poor_man_lakehouse.spark_connector import (
    CatalogType,
    get_spark_builder,
    retrieve_current_spark_session,
)

__all__ = [
    # Catalog
    "LakehouseCatalogType",
    "get_catalog",
    # Config
    "Settings",
    "get_settings",
    "reload_settings",
    "settings",
    # Connectors
    "DremioConnection",
    "LakehouseConnection",
    # Spark
    "CatalogType",
    "get_spark_builder",
    "retrieve_current_spark_session",
]
```

- [ ] **Step 2: Delete old modules**

```bash
rm src/poor_man_lakehouse/catalog_browser.py
rm -rf src/poor_man_lakehouse/pyiceberg_connector
rm -rf src/poor_man_lakehouse/polars_connector
rm -rf src/poor_man_lakehouse/ibis_connector
```

- [ ] **Step 3: Delete old tests**

```bash
rm tests/unit/test_catalog_browser.py
rm tests/unit/test_pyiceberg_connector.py
rm tests/unit/test_polars_connector.py
rm tests/unit/test_ibis_connector.py
```

- [ ] **Step 4: Run all tests**

Run: `uv run pytest tests/unit/ -v`
Expected: All tests PASS (test_catalog, test_lakehouse, test_config, test_spark_builder, test_dremio_connector)

- [ ] **Step 5: Run linting on the full codebase**

Run: `uv run ruff format src/ tests/ && uv run ruff check src/ tests/`
Expected: No issues

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: remove old connectors, update __init__.py exports"
```

---

### Task 7: Update CLAUDE.md and final verification

**Files:**
- Modify: `.claude/CLAUDE.md`

- [ ] **Step 1: Update CLAUDE.md Architecture section**

Update the Architecture section in `.claude/CLAUDE.md` to reflect the new structure. Replace the four connector subpackage descriptions with:

```markdown
- **`catalog.py`** — `get_catalog()` factory returns a PyIceberg `Catalog` for any supported backend (Lakekeeper, Nessie, PostgreSQL, Glue). Single source of truth for catalog config — all other modules compose it.
- **`lakehouse.py`** — `LakehouseConnection` is the unified lightweight connector. Provides catalog browsing (namespaces, tables, schemas, snapshots), native scans (Polars LazyFrame, Arrow), DuckDB engine with attached Iceberg catalog, Ibis multi-engine wrappers (DuckDB/Polars/PySpark), SQL execution, and DuckDB-based writes. All backed by `get_catalog()`.
- **`spark_connector/`** — SparkSession builders for each catalog type. Uses an abstract `SparkBuilder` base class with subclasses per catalog (`PostgresCatalogSparkBuilder`, `NessieCatalogSparkBuilder`, `LakekeeperCatalogSparkBuilder`, `GlueCatalogSparkBuilder`). Factory function `get_spark_builder(catalog_type)` selects the right one.
- **`dremio_connector/`** — `DremioConnection` for Arrow Flight queries against Dremio. Output methods: `to_polars()`, `to_duckdb()`, `to_pandas()`, `to_arrow()`.
```

Also update the top-level `__init__.py` description:

```markdown
**Top-level `__init__.py`** re-exports: `get_catalog`, `LakehouseCatalogType`, `LakehouseConnection`, `DremioConnection`, `CatalogType`, `get_spark_builder`, `retrieve_current_spark_session`, config utilities.
```

Remove references to `IbisConnection`, `PolarsClient`, `PyIcebergClient`, `CatalogBrowser`, `load_sql_magic`, and `unity_catalog` throughout the file.

Update Key Constraints to remove the `IbisConnection` lakekeeper/glue constraint.

- [ ] **Step 2: Run the full test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 3: Run full lint**

Run: `just lint`
Expected: No issues

- [ ] **Step 4: Commit**

```bash
git add .claude/CLAUDE.md
git commit -m "docs: update CLAUDE.md for catalog-agnostic refactor"
```
