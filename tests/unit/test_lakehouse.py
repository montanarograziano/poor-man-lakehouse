"""Unit tests for the lakehouse module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit


class TestLakehouseConnectionInit:
    """Tests for LakehouseConnection initialization."""

    @patch("poor_man_lakehouse.lakehouse.settings")
    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_init_creates_catalog(self, mock_get_catalog, mock_settings):
        """Test that init calls get_catalog with resolved catalog type."""
        mock_settings.CATALOG = "nessie"
        mock_get_catalog.return_value = MagicMock()
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        mock_get_catalog.assert_called_once_with("nessie")
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


class TestLakehouseConnectionSQL:
    """Tests for SQL execution."""

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_sql_raises_for_unsupported_engine(self, mock_get_catalog):
        """Test sql() raises for unsupported engines."""
        mock_get_catalog.return_value = MagicMock()
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        with pytest.raises(ValueError, match="SQL execution only supports"):
            conn.sql("SELECT 1", "polars")  # pyright: ignore[reportArgumentType]

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_sql_duckdb_delegates_to_connection(self, mock_get_catalog):
        """Test sql() with duckdb engine delegates to duckdb_connection."""
        mock_get_catalog.return_value = MagicMock()
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        mock_duck = MagicMock()
        mock_duck.sql.return_value = MagicMock()
        conn.__dict__["duckdb_connection"] = mock_duck

        result = conn.sql("SELECT 1")
        mock_duck.sql.assert_called_once_with("SELECT 1")
        assert result is mock_duck.sql.return_value


class TestLakehouseConnectionWrite:
    """Tests for write operations."""

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_write_table_raises_for_invalid_mode(self, mock_get_catalog):
        """Test write_table raises for unsupported write modes."""
        mock_get_catalog.return_value = MagicMock()
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        with pytest.raises(ValueError, match="Unsupported write mode"):
            conn.write_table("default", "test", mode="invalid")  # pyright: ignore[reportArgumentType]

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_write_table_raises_when_no_data_or_query(self, mock_get_catalog):
        """Test write_table raises when neither data nor query provided."""
        mock_get_catalog.return_value = MagicMock()
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        with pytest.raises(ValueError, match="Either 'data' or 'query' must be provided"):
            conn.write_table("default", "test")

    @patch("poor_man_lakehouse.lakehouse.settings")
    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_write_table_append_with_query(self, mock_get_catalog, mock_settings):
        """Test write_table appends data from a SQL query."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_get_catalog.return_value = MagicMock()
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        mock_duck = MagicMock()
        conn.__dict__["duckdb_connection"] = mock_duck

        conn.write_table("default", "users", query="SELECT 1 AS id, 'Alice' AS name")
        calls = [str(c) for c in mock_duck.raw_sql.call_args_list]
        assert any("INSERT INTO" in c for c in calls)
        assert not any("DELETE FROM" in c for c in calls)

    @patch("poor_man_lakehouse.lakehouse.settings")
    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_write_table_overwrite_deletes_first(self, mock_get_catalog, mock_settings):
        """Test write_table in overwrite mode deletes before inserting."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_get_catalog.return_value = MagicMock()
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        mock_duck = MagicMock()
        conn.__dict__["duckdb_connection"] = mock_duck

        conn.write_table("default", "users", query="SELECT 1 AS id", mode="overwrite")
        calls = [str(c) for c in mock_duck.raw_sql.call_args_list]
        assert any("DELETE FROM" in c for c in calls)
        assert any("INSERT INTO" in c for c in calls)

    @patch("poor_man_lakehouse.lakehouse.settings")
    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_create_table_calls_raw_sql(self, mock_get_catalog, mock_settings):
        """Test create_table issues CREATE TABLE IF NOT EXISTS."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_get_catalog.return_value = MagicMock()
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        mock_duck = MagicMock()
        conn.__dict__["duckdb_connection"] = mock_duck

        conn.create_table("default", "users", "id INTEGER, name VARCHAR")
        mock_duck.raw_sql.assert_called_once()
        call_arg = str(mock_duck.raw_sql.call_args)
        assert "CREATE TABLE IF NOT EXISTS" in call_arg
        assert "lakekeeper.default.users" in call_arg


class TestLakehouseConnectionIbis:
    """Tests for Ibis engine access methods."""

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_ibis_duckdb_returns_duckdb_connection(self, mock_get_catalog):
        """Test ibis_duckdb returns the cached duckdb_connection."""
        mock_get_catalog.return_value = MagicMock()
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        mock_duck = MagicMock()
        conn.__dict__["duckdb_connection"] = mock_duck

        result = conn.ibis_duckdb()
        assert result is mock_duck

    @patch("poor_man_lakehouse.lakehouse.pl")
    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_ibis_polars_registers_table(self, mock_get_catalog, mock_pl):
        """Test ibis_polars scans table and registers it in a Polars Ibis backend."""
        import ibis

        mock_table = MagicMock()
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_get_catalog.return_value = mock_catalog
        mock_lazyframe = MagicMock()
        mock_pl.scan_iceberg.return_value = mock_lazyframe
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        with patch.object(ibis.polars, "connect") as mock_polars_connect:
            mock_polars_backend = MagicMock()
            mock_polars_connect.return_value = mock_polars_backend

            result = conn.ibis_polars("default", "users")

            mock_polars_backend.create_table.assert_called_once_with("default.users", mock_lazyframe, overwrite=True)
            assert result is mock_polars_backend


class TestLakehouseConnectionCreateNamespace:
    """Tests for create_namespace method."""

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_create_namespace(self, mock_get_catalog):
        """Test create_namespace delegates to catalog."""
        mock_catalog = MagicMock()
        mock_get_catalog.return_value = mock_catalog
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        conn.create_namespace("staging")
        mock_catalog.create_namespace.assert_called_once_with("staging")


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
        conn.__dict__["duckdb_connection"] = MagicMock()
        conn.close()
        assert "duckdb_connection" not in conn.__dict__

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_repr(self, mock_get_catalog):
        """Test repr includes catalog type."""
        mock_get_catalog.return_value = MagicMock()
        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection(catalog_type="lakekeeper")
        assert "lakekeeper" in repr(conn)
