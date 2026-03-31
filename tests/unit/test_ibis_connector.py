"""Unit tests for the ibis_connector module."""

from unittest.mock import MagicMock, patch

import pytest


class TestIbisConnectionInit:
    """Tests for IbisConnection initialization."""

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_raises_if_catalog_not_supported(self, mock_settings):
        """Test that IbisConnection raises ValueError for unsupported catalogs."""
        mock_settings.CATALOG = "nessie"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        with pytest.raises(ValueError, match="IbisConnection supports catalogs"):
            IbisConnection()

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_init_with_lakekeeper_catalog(self, mock_settings):
        """Test that IbisConnection initializes when catalog is lakekeeper."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        assert conn._catalog_name == "lakekeeper"
        assert conn._catalog_type == "lakekeeper"
        assert conn._lakekeeper_endpoint == "http://lakekeeper:8181/catalog"

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_init_with_glue_catalog(self, mock_settings):
        """Test that IbisConnection initializes when catalog is glue."""
        mock_settings.CATALOG = "glue"
        mock_settings.CATALOG_NAME = "glue"
        mock_settings.AWS_DEFAULT_REGION = "us-east-1"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        assert conn._catalog_name == "glue"
        assert conn._catalog_type == "glue"
        assert conn._lakekeeper_endpoint == ""


class TestIbisConnectionGetConnection:
    """Tests for get_connection method."""

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_get_connection_raises_for_unsupported_engine(self, mock_settings):
        """Test that get_connection raises for unsupported engines."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        with pytest.raises(ValueError, match="Unsupported engine"):
            conn.get_connection("invalid")  # type: ignore[arg-type]


class TestIbisConnectionSQL:
    """Tests for SQL execution methods."""

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_sql_raises_for_polars_engine(self, mock_settings):
        """Test that sql() raises for polars engine (SQL not supported)."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        with pytest.raises(ValueError, match="SQL execution only supports"):
            conn.sql("SELECT 1", "polars")  # type: ignore[arg-type]

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_set_current_database_raises_for_polars(self, mock_settings):
        """Test that set_current_database raises for polars engine."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        with pytest.raises(ValueError, match="does not support database switching"):
            conn.set_current_database("default", "polars")  # type: ignore[arg-type]


class TestIbisConnectionReadTable:
    """Tests for read_table method."""

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_read_table_raises_for_unsupported_engine(self, mock_settings):
        """Test that read_table raises for unsupported engines."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        with pytest.raises(ValueError, match="Unsupported engine for read_table"):
            conn.read_table("default", "test_table", "invalid")  # type: ignore[arg-type]

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_read_table_pyspark_wraps_errors(self, mock_settings):
        """Test that read_table wraps PySpark errors in ValueError."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"

        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        # Mock the pyspark cached_property to raise
        mock_pyspark = MagicMock()
        mock_pyspark.table.side_effect = RuntimeError("table not found")
        conn.__dict__["_pyspark_connection"] = mock_pyspark

        with pytest.raises(ValueError, match="Could not read table"):
            conn.read_table("default", "test_table", "pyspark")


class TestIbisConnectionWriteTable:
    """Tests for write_table method."""

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_write_table_raises_for_unsupported_engine(self, mock_settings):
        """Test that write_table raises ValueError for non-DuckDB engines."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        with pytest.raises(ValueError, match="only supported with DuckDB"):
            conn.write_table("default", "test", "pyspark", mode="append")

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_write_table_raises_for_invalid_mode(self, mock_settings):
        """Test that write_table raises ValueError for unsupported write modes."""
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
        """Test that IbisConnection works as a context manager."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        with IbisConnection() as conn:
            assert conn._catalog_name == "lakekeeper"

    @patch("poor_man_lakehouse.ibis_connector.builder.settings")
    def test_close_clears_cached_properties(self, mock_settings):
        """Test that close() removes cached connection properties."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        from poor_man_lakehouse.ibis_connector.builder import IbisConnection

        conn = IbisConnection()
        conn.close()
        assert "_duckdb_connection" not in conn.__dict__
        assert "_pyspark_connection" not in conn.__dict__
        assert "_polars_connection" not in conn.__dict__
