"""Unit tests for the config module."""

from unittest.mock import patch

import pytest

from poor_man_lakehouse.config import Settings, SettingsError, get_settings, reload_settings


class TestSettings:
    """Tests for the Settings class."""

    def test_default_values(self, monkeypatch):
        """Test that Settings has expected default values when no .env or env vars are loaded."""
        # Clear env vars that would override defaults
        for key in ("AWS_DEFAULT_REGION", "CATALOG", "CATALOG_NAME"):
            monkeypatch.delenv(key, raising=False)

        test_settings = Settings(_env_file=None)
        assert test_settings.APP_NAME == "Poor Man Lakehouse"
        assert test_settings.PROJECT_NAME == "poor-man-lakehouse"
        assert test_settings.AWS_DEFAULT_REGION == "eu-central-1"
        assert test_settings.CATALOG == "nessie"
        assert test_settings.CATALOG_NAME == "nessie"

    def test_s3_storage_options_populated_after_init(self):
        """Test S3 storage options are populated via model_validator after init."""
        test_settings = Settings(_env_file=None)
        # model_validator runs _configure_data_path automatically
        assert "AWS_ACCESS_KEY_ID" in test_settings.S3_STORAGE_OPTIONS
        assert "AWS_ALLOW_HTTP" in test_settings.S3_STORAGE_OPTIONS
        assert test_settings.S3_STORAGE_OPTIONS["AWS_ALLOW_HTTP"] == "true"
        assert test_settings.S3_STORAGE_OPTIONS["allow_http"] == "true"

    def test_configure_data_path_populates_s3_options(self):
        """Test _configure_data_path populates S3 storage options correctly."""
        test_settings = Settings(
            _env_file=None,
            AWS_ACCESS_KEY_ID="test-key",
            AWS_SECRET_ACCESS_KEY="test-secret",  # noqa: S106
        )

        assert test_settings.S3_STORAGE_OPTIONS["AWS_ACCESS_KEY_ID"] == "test-key"
        assert test_settings.S3_STORAGE_OPTIONS["AWS_SECRET_ACCESS_KEY"] == "test-secret"  # noqa: S105
        assert test_settings.S3_STORAGE_OPTIONS["AWS_ALLOW_HTTP"] == "true"

    def test_configure_data_path_populates_iceberg_options_nessie(self):
        """Test _configure_data_path populates Iceberg options for non-lakekeeper catalogs."""
        test_settings = Settings(
            _env_file=None,
            AWS_ACCESS_KEY_ID="test-key",
            AWS_SECRET_ACCESS_KEY="test-secret",  # noqa: S106
            AWS_ENDPOINT_URL="http://minio:9000",
            CATALOG="nessie",
            BUCKET_NAME="warehouse",
        )

        assert test_settings.ICEBERG_STORAGE_OPTIONS["s3.access-key-id"] == "test-key"
        assert test_settings.ICEBERG_STORAGE_OPTIONS["s3.secret-access-key"] == "test-secret"  # noqa: S105
        assert test_settings.ICEBERG_STORAGE_OPTIONS["s3.endpoint"] == "http://minio:9000"
        assert test_settings.ICEBERG_STORAGE_OPTIONS["warehouse"] == "s3://warehouse/"

    def test_configure_data_path_populates_iceberg_options_lakekeeper(self):
        """Test _configure_data_path uses LAKEKEEPER_WAREHOUSE when catalog is lakekeeper."""
        test_settings = Settings(
            _env_file=None,
            CATALOG="lakekeeper",
            LAKEKEEPER_WAREHOUSE="my-warehouse",
        )

        assert test_settings.ICEBERG_STORAGE_OPTIONS["warehouse"] == "my-warehouse"

    def test_warehouse_bucket_computed_from_bucket_name(self):
        """Test WAREHOUSE_BUCKET is computed from BUCKET_NAME at instance time."""
        test_settings = Settings(_env_file=None, BUCKET_NAME="my-bucket")
        assert test_settings.WAREHOUSE_BUCKET == "s3://my-bucket/"

    def test_settings_path_computed_from_repo_path(self):
        """Test SETTINGS_PATH is computed from REPO_PATH."""
        test_settings = Settings(_env_file=None, REPO_PATH="/tmp/test")  # noqa: S108
        assert test_settings.SETTINGS_PATH == "/tmp/test/settings"  # noqa: S108


class TestGetSettings:
    """Tests for the get_settings function."""

    def test_get_settings_returns_settings_instance(self):
        """Test get_settings returns a Settings instance."""
        get_settings.cache_clear()
        result = get_settings()
        assert isinstance(result, Settings)

    def test_get_settings_is_cached(self):
        """Test get_settings returns the same cached instance."""
        get_settings.cache_clear()
        settings1 = get_settings()
        settings2 = get_settings()
        assert settings1 is settings2

    def test_reload_settings_clears_cache(self):
        """Test reload_settings clears the cache and returns new instance."""
        get_settings.cache_clear()
        _initial_settings = get_settings()  # noqa: F841
        reloaded_settings = reload_settings()
        current_settings = get_settings()
        assert reloaded_settings is current_settings

    @patch("poor_man_lakehouse.config.Settings")
    def test_get_settings_raises_settings_error_on_failure(self, mock_settings_class):
        """Test get_settings raises SettingsError when initialization fails."""
        mock_settings_class.side_effect = Exception("Test error")
        get_settings.cache_clear()

        with pytest.raises(SettingsError, match="Error importing settings"):
            get_settings()


class TestSettingsError:
    """Tests for the SettingsError exception."""

    def test_settings_error_is_exception(self):
        """Test SettingsError is a proper Exception subclass."""
        error = SettingsError("test message")
        assert isinstance(error, Exception)
        assert str(error) == "test message"


class TestRequireCatalog:
    """Tests for require_catalog helper."""

    def test_require_catalog_passes_for_matching(self, monkeypatch):
        """Test require_catalog passes when catalog matches."""
        monkeypatch.delenv("CATALOG", raising=False)
        from poor_man_lakehouse.config import require_catalog

        s = Settings(_env_file=None)  # defaults to CATALOG="nessie"
        require_catalog("nessie", connector_name="test", current_settings=s)

    def test_require_catalog_raises_for_mismatch(self, monkeypatch):
        """Test require_catalog raises when catalog doesn't match."""
        monkeypatch.delenv("CATALOG", raising=False)
        from poor_man_lakehouse.config import require_catalog

        s = Settings(_env_file=None)  # defaults to CATALOG="nessie"
        with pytest.raises(ValueError, match="requires 'lakekeeper' catalog"):
            require_catalog("lakekeeper", connector_name="IbisConnection", current_settings=s)
