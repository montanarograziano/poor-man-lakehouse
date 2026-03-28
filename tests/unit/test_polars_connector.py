"""Unit tests for the polars_connector module."""

from unittest.mock import MagicMock, patch

import pytest

from poor_man_lakehouse.polars_connector.client import PolarsClient, TableInfo


class TestTableInfo:
    """Tests for the TableInfo dataclass."""

    def test_full_name(self):
        """Test fully qualified name property."""
        info = TableInfo(catalog="unity", namespace="default", name="users")
        assert info.full_name == "unity.default.users"

    def test_default_location_is_none(self):
        """Test that location defaults to None."""
        info = TableInfo(catalog="c", namespace="n", name="t")
        assert info.location is None

    def test_with_location(self):
        """Test TableInfo with explicit location."""
        info = TableInfo(catalog="c", namespace="n", name="t", location="s3://bucket/path")
        assert info.location == "s3://bucket/path"


class TestPolarsClientInit:
    """Tests for PolarsClient initialization."""

    @patch("poor_man_lakehouse.polars_connector.client.settings")
    @patch("poor_man_lakehouse.polars_connector.client.pl.Catalog")
    def test_init_with_defaults(self, mock_catalog_cls, mock_settings):
        """Test initialization with default settings."""
        mock_settings.UNITY_CATALOG_URI = "http://localhost:8080/"
        mock_settings.S3_STORAGE_OPTIONS = {"key": "value"}
        mock_catalog_instance = MagicMock()
        mock_catalog_cls.return_value = mock_catalog_instance

        client = PolarsClient()

        assert client.unity_catalog_uri == "http://localhost:8080/"
        assert client.storage_options == {"key": "value"}
        mock_catalog_cls.assert_called_once_with("http://localhost:8080/", require_https=False)

    @patch("poor_man_lakehouse.polars_connector.client.settings")
    @patch("poor_man_lakehouse.polars_connector.client.pl.Catalog")
    def test_init_with_custom_uri(self, mock_catalog_cls, mock_settings):
        """Test initialization with custom URI."""
        mock_settings.S3_STORAGE_OPTIONS = {}
        mock_catalog_cls.return_value = MagicMock()

        client = PolarsClient(unity_catalog_uri="http://custom:9090")
        assert client.unity_catalog_uri == "http://custom:9090"


class TestPolarsClientCache:
    """Tests for the table scan cache."""

    @patch("poor_man_lakehouse.polars_connector.client.settings")
    @patch("poor_man_lakehouse.polars_connector.client.pl.Catalog")
    def test_clear_cache(self, mock_catalog_cls, mock_settings):
        """Test cache clearing."""
        mock_settings.UNITY_CATALOG_URI = "http://localhost:8080/"
        mock_settings.S3_STORAGE_OPTIONS = {}
        mock_catalog_cls.return_value = MagicMock()

        client = PolarsClient()
        client._table_cache["test"] = MagicMock()
        assert len(client._table_cache) == 1

        client.clear_cache()
        assert len(client._table_cache) == 0


class TestPolarsClientSQL:
    """Tests for SQL execution."""

    @patch("poor_man_lakehouse.polars_connector.client.settings")
    @patch("poor_man_lakehouse.polars_connector.client.pl.Catalog")
    def test_sql_raises_for_empty_tables(self, mock_catalog_cls, mock_settings):
        """Test that sql() raises when no tables found in query."""
        mock_settings.UNITY_CATALOG_URI = "http://localhost:8080/"
        mock_settings.S3_STORAGE_OPTIONS = {}
        mock_catalog_cls.return_value = MagicMock()

        client = PolarsClient()
        with pytest.raises(ValueError, match="No tables found in query"):
            client.sql("SELECT 1")

    @patch("poor_man_lakehouse.polars_connector.client.settings")
    @patch("poor_man_lakehouse.polars_connector.client.pl.Catalog")
    def test_sql_raises_for_unqualified_table(self, mock_catalog_cls, mock_settings):
        """Test that sql() raises when table is not fully qualified."""
        mock_settings.UNITY_CATALOG_URI = "http://localhost:8080/"
        mock_settings.S3_STORAGE_OPTIONS = {}
        mock_catalog_cls.return_value = MagicMock()

        client = PolarsClient()
        with pytest.raises(ValueError, match="must be fully qualified"):
            client.sql("SELECT * FROM just_table_name")


class TestPolarsClientRepr:
    """Tests for string representation."""

    @patch("poor_man_lakehouse.polars_connector.client.settings")
    @patch("poor_man_lakehouse.polars_connector.client.pl.Catalog")
    def test_repr(self, mock_catalog_cls, mock_settings):
        """Test __repr__ output."""
        mock_settings.UNITY_CATALOG_URI = "http://localhost:8080/"
        mock_settings.S3_STORAGE_OPTIONS = {}
        mock_catalog_cls.return_value = MagicMock()

        client = PolarsClient()
        assert repr(client) == "PolarsClient(uri='http://localhost:8080/')"


class TestPolarsClientGetTableInfo:
    """Tests for get_table_info method."""

    @patch("poor_man_lakehouse.polars_connector.client.settings")
    @patch("poor_man_lakehouse.polars_connector.client.pl.Catalog")
    def test_get_table_info(self, mock_catalog_cls, mock_settings):
        """Test get_table_info returns correct TableInfo."""
        mock_settings.UNITY_CATALOG_URI = "http://localhost:8080/"
        mock_settings.S3_STORAGE_OPTIONS = {}
        mock_catalog_cls.return_value = MagicMock()

        client = PolarsClient()
        info = client.get_table_info("unity", "default", "users")
        assert info.catalog == "unity"
        assert info.namespace == "default"
        assert info.name == "users"
        assert info.full_name == "unity.default.users"
