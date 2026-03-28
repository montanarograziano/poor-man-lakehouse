"""Unit tests for the pyiceberg_connector module."""

from unittest.mock import MagicMock, patch


class TestPyIcebergClientInit:
    """Tests for PyIcebergClient initialization."""

    @patch("poor_man_lakehouse.pyiceberg_connector.client.settings")
    @patch("poor_man_lakehouse.pyiceberg_connector.client.load_catalog")
    def test_init_creates_catalog(self, mock_load_catalog, mock_settings):
        """Test that init creates a catalog from default settings."""
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
        """Test that custom URI and catalog name override defaults."""
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
        """Test listing namespaces returns catalog namespaces."""
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
        """Test listing tables in a namespace."""
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
        """Test loading a table by namespace and name."""
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


class TestPyIcebergClientRepr:
    """Tests for string representation."""

    @patch("poor_man_lakehouse.pyiceberg_connector.client.settings")
    @patch("poor_man_lakehouse.pyiceberg_connector.client.load_catalog")
    def test_repr(self, mock_load_catalog, mock_settings):
        """Test repr includes catalog name."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {}
        mock_load_catalog.return_value = MagicMock()
        from poor_man_lakehouse.pyiceberg_connector.client import PyIcebergClient

        client = PyIcebergClient()
        assert "lakekeeper" in repr(client)
