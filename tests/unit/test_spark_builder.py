"""Unit tests for the Spark builder module."""

from unittest.mock import MagicMock, patch

import pytest

from poor_man_lakehouse.spark_connector.builder import (
    COMMON_PACKAGES,
    CatalogType,
    DeltaUnityCatalogSparkBuilder,
    LakekeeperCatalogSparkBuilder,
    NessieCatalogSparkBuilder,
    PostgresCatalogSparkBuilder,
    get_spark_builder,
)


class TestCatalogType:
    """Tests for the CatalogType enum."""

    def test_catalog_type_values(self):
        """Test CatalogType enum has expected values."""
        assert CatalogType.POSTGRES.value == "postgres"
        assert CatalogType.UNITY_CATALOG.value == "unity_catalog"
        assert CatalogType.NESSIE.value == "nessie"
        assert CatalogType.LAKEKEEPER.value == "lakekeeper"

    def test_catalog_type_from_string(self):
        """Test CatalogType can be created from string."""
        assert CatalogType("postgres") == CatalogType.POSTGRES
        assert CatalogType("nessie") == CatalogType.NESSIE

    def test_catalog_type_invalid_raises(self):
        """Test invalid catalog type raises ValueError."""
        with pytest.raises(ValueError):
            CatalogType("invalid")


class TestGetSparkBuilder:
    """Tests for the get_spark_builder factory function."""

    def test_returns_postgres_builder(self):
        """Test get_spark_builder returns PostgresCatalogSparkBuilder."""
        builder = get_spark_builder(CatalogType.POSTGRES)
        assert isinstance(builder, PostgresCatalogSparkBuilder)

    def test_returns_postgres_builder_from_string(self):
        """Test get_spark_builder accepts string input."""
        builder = get_spark_builder("postgres")
        assert isinstance(builder, PostgresCatalogSparkBuilder)

    def test_returns_unity_catalog_builder(self):
        """Test get_spark_builder returns DeltaUnityCatalogSparkBuilder."""
        builder = get_spark_builder(CatalogType.UNITY_CATALOG)
        assert isinstance(builder, DeltaUnityCatalogSparkBuilder)

    def test_returns_nessie_builder(self):
        """Test get_spark_builder returns NessieCatalogSparkBuilder."""
        builder = get_spark_builder(CatalogType.NESSIE)
        assert isinstance(builder, NessieCatalogSparkBuilder)

    def test_returns_lakekeeper_builder(self):
        """Test get_spark_builder returns LakekeeperCatalogSparkBuilder."""
        builder = get_spark_builder(CatalogType.LAKEKEEPER)
        assert isinstance(builder, LakekeeperCatalogSparkBuilder)

    def test_raises_for_invalid_catalog_string(self):
        """Test get_spark_builder raises ValueError for invalid string."""
        with pytest.raises(ValueError, match="Unsupported catalog"):
            get_spark_builder("invalid_catalog")


class TestSparkBuilderBase:
    """Tests for the SparkBuilder base class functionality."""

    def test_default_app_name(self):
        """Test default app name is set."""
        builder = PostgresCatalogSparkBuilder()
        assert builder._app_name == "Poor Man Lakehouse"

    def test_custom_app_name(self):
        """Test custom app name can be set."""
        builder = PostgresCatalogSparkBuilder(app_name="Custom App")
        assert builder._app_name == "Custom App"

    def test_get_packages_returns_common_packages(self):
        """Test _get_packages returns a copy of COMMON_PACKAGES."""
        builder = PostgresCatalogSparkBuilder()
        packages = builder._get_packages()
        assert packages == COMMON_PACKAGES
        # Should be a copy, not the same list
        assert packages is not COMMON_PACKAGES

    @patch("poor_man_lakehouse.spark_connector.builder.settings")
    def test_catalog_name_property(self, mock_settings):
        """Test catalog_name property returns settings.CATALOG_NAME."""
        mock_settings.CATALOG_NAME = "test-catalog"
        builder = PostgresCatalogSparkBuilder()
        assert builder.catalog_name == "test-catalog"


class TestPostgresCatalogSparkBuilder:
    """Tests for PostgresCatalogSparkBuilder."""

    @patch("poor_man_lakehouse.spark_connector.builder.settings")
    @patch("poor_man_lakehouse.spark_connector.builder.SparkSession")
    def test_configure_catalog_sets_jdbc_config(self, _mock_spark, mock_settings):
        """Test _configure_catalog sets JDBC configuration."""
        mock_settings.CATALOG_NAME = "postgres_catalog"
        mock_settings.POSTGRES_HOST = "localhost"
        mock_settings.POSTGRES_DB = "test_db"
        mock_settings.POSTGRES_USER = "user"
        mock_settings.POSTGRES_PASSWORD = "pass"  # noqa: S105
        mock_settings.WAREHOUSE_BUCKET = "s3a://warehouse/"

        builder = PostgresCatalogSparkBuilder()
        mock_builder = MagicMock()
        mock_builder.config.return_value = mock_builder

        builder._configure_catalog(mock_builder)

        # Verify config was called with JDBC settings
        config_calls = [str(call) for call in mock_builder.config.call_args_list]
        assert any("jdbc:postgresql://localhost/test_db" in str(call) for call in config_calls)
        assert any("postgres_catalog" in str(call) for call in config_calls)


class TestNessieCatalogSparkBuilder:
    """Tests for NessieCatalogSparkBuilder."""

    @patch("poor_man_lakehouse.spark_connector.builder.settings")
    def test_configure_catalog_sets_nessie_config(self, mock_settings):
        """Test _configure_catalog sets Nessie catalog configuration."""
        mock_settings.CATALOG_NAME = "nessie"
        mock_settings.NESSIE_NATIVE_URI = "http://nessie:19120/api/v2"
        mock_settings.WAREHOUSE_BUCKET = "s3a://warehouse/"
        mock_settings.AWS_ENDPOINT_URL = "http://minio:9000"
        mock_settings.AWS_ACCESS_KEY_ID = "minioadmin"
        mock_settings.AWS_SECRET_ACCESS_KEY = "miniopassword"  # noqa: S105

        builder = NessieCatalogSparkBuilder()
        mock_builder = MagicMock()
        mock_builder.config.return_value = mock_builder

        builder._configure_catalog(mock_builder)

        # Verify Nessie catalog config was set (uses NessieCatalog, not REST)
        config_calls = [str(call) for call in mock_builder.config.call_args_list]
        assert any("NessieCatalog" in str(call) for call in config_calls)
        assert any("nessie" in str(call) for call in config_calls)


class TestLakekeeperCatalogSparkBuilder:
    """Tests for LakekeeperCatalogSparkBuilder."""

    @patch("poor_man_lakehouse.spark_connector.builder.settings")
    def test_configure_catalog_uses_lakekeeper_uri(self, mock_settings):
        """Test _configure_catalog uses Lakekeeper server URI."""
        mock_settings.CATALOG_NAME = "lakekeeper"
        mock_settings.LAKEKEEPER_SERVER_URI = "http://lakekeeper:8181"
        mock_settings.WAREHOUSE_BUCKET = "s3a://warehouse/"
        mock_settings.AWS_ENDPOINT_URL = "http://minio:9000"

        builder = LakekeeperCatalogSparkBuilder()
        mock_builder = MagicMock()
        mock_builder.config.return_value = mock_builder

        builder._configure_catalog(mock_builder)

        # Verify Lakekeeper URI was used
        config_calls = [str(call) for call in mock_builder.config.call_args_list]
        assert any("http://lakekeeper:8181" in str(call) for call in config_calls)


class TestDeltaUnityCatalogSparkBuilder:
    """Tests for DeltaUnityCatalogSparkBuilder."""

    def test_get_packages_includes_unity_catalog(self):
        """Test _get_packages includes Unity Catalog package."""
        builder = DeltaUnityCatalogSparkBuilder()
        packages = builder._get_packages()

        # Should include Unity Catalog package
        assert any("unitycatalog-spark" in pkg for pkg in packages)
        # Should also include common packages
        assert any("iceberg-spark-runtime" in pkg for pkg in packages)


class TestCommonPackages:
    """Tests for the common packages configuration."""

    def test_common_packages_include_iceberg(self):
        """Test COMMON_PACKAGES includes Iceberg runtime."""
        assert any("iceberg-spark-runtime" in pkg for pkg in COMMON_PACKAGES)

    def test_common_packages_include_aws_bundle(self):
        """Test COMMON_PACKAGES includes AWS bundle."""
        assert any("iceberg-aws-bundle" in pkg for pkg in COMMON_PACKAGES)

    def test_common_packages_include_hadoop_aws(self):
        """Test COMMON_PACKAGES includes Hadoop AWS."""
        assert any("hadoop-aws" in pkg for pkg in COMMON_PACKAGES)

    def test_common_packages_include_postgresql(self):
        """Test COMMON_PACKAGES includes PostgreSQL driver."""
        assert any("postgresql" in pkg for pkg in COMMON_PACKAGES)
