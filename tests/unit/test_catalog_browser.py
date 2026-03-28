"""Unit tests for the catalog_browser module."""

from unittest.mock import MagicMock, patch


class TestCatalogBrowserInit:
    """Tests for CatalogBrowser initialization."""

    @patch("poor_man_lakehouse.catalog_browser.load_catalog")
    @patch("poor_man_lakehouse.catalog_browser.settings")
    def test_init_uses_settings(self, mock_settings, mock_load_catalog):
        """Test that CatalogBrowser uses settings defaults."""
        mock_settings.CATALOG = "lakekeeper"
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181/catalog"
        mock_settings.ICEBERG_STORAGE_OPTIONS = {"s3.endpoint": "http://minio:9000"}
        mock_load_catalog.return_value = MagicMock()
        from poor_man_lakehouse.catalog_browser import CatalogBrowser

        CatalogBrowser()
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
    """Tests for catalog URI resolution."""

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
