"""Unit tests for the catalog module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit


class TestGetCatalogLakekeeper:
    """Tests for get_catalog with lakekeeper catalog type."""

    @patch("poor_man_lakehouse.catalog.load_catalog")
    @patch("poor_man_lakehouse.catalog.settings")
    def test_lakekeeper_creates_rest_catalog(self, mock_settings, mock_load_catalog):
        """Test that lakekeeper catalog creates a REST catalog with correct config."""
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
        """Test that passing catalog_type='lakekeeper' explicitly works."""
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
        """Test that nessie catalog creates a REST catalog with correct URI."""
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
        """Test that postgres catalog creates a SQL catalog with correct URI."""
        mock_settings.CATALOG = "postgres"
        mock_settings.CATALOG_NAME = "postgres"
        mock_settings.POSTGRES_HOST = "localhost"
        mock_settings.POSTGRES_DB = "lakehouse_db"
        mock_settings.POSTGRES_USER = "postgres"
        mock_settings.POSTGRES_PASSWORD = "password"
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
        """Test that glue catalog creates a Glue catalog with correct config."""
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
        assert kwargs["warehouse"] == "s3://my-data-lake/"
        assert "glue.id" not in kwargs

    @patch("poor_man_lakehouse.catalog.load_catalog")
    @patch("poor_man_lakehouse.catalog.settings")
    def test_glue_includes_catalog_id_when_set(self, mock_settings, mock_load_catalog):
        """Test that glue.id is included in config when GLUE_CATALOG_ID is set."""
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
        """Test that unsupported CATALOG env value raises ValueError."""
        mock_settings.CATALOG = "unity_catalog"

        from poor_man_lakehouse.catalog import get_catalog

        with pytest.raises(ValueError, match="Unsupported catalog type"):
            get_catalog()

    @patch("poor_man_lakehouse.catalog.settings")
    def test_raises_for_invalid_explicit_type(self, mock_settings):
        """Test that invalid explicit catalog_type raises ValueError."""
        from poor_man_lakehouse.catalog import get_catalog

        with pytest.raises(ValueError, match="Unsupported catalog type"):
            get_catalog(catalog_type="invalid")
