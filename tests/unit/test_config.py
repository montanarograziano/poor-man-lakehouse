"""Unit tests for the config module."""

from unittest.mock import patch

import pytest

from poor_man_lakehouse.config import Settings, SettingsError, get_settings, reload_settings


class TestSettings:
    """Tests for the Settings class."""

    def test_default_values(self):
        """Test that Settings has expected default values.

        Note: CATALOG and CATALOG_NAME may be overridden by .env file,
        so we only test values that are not typically configured in .env.
        """
        test_settings = Settings()
        assert test_settings.APP_NAME == "Poor Man Lakehouse"
        assert test_settings.PROJECT_NAME == "poor-man-lakehouse"
        assert test_settings.AWS_DEFAULT_REGION == "eu-central-1"
        # CATALOG may be overridden by .env, just check it's a valid string
        assert isinstance(test_settings.CATALOG, str)
        assert len(test_settings.CATALOG) > 0

    def test_s3_storage_options_empty_by_default(self):
        """Test S3 storage options are empty before configuration."""
        settings = Settings()
        assert settings.S3_STORAGE_OPTIONS == {}
        assert settings.ICEBERG_STORAGE_OPTIONS == {}

    def test_configure_data_path_populates_s3_options(self):
        """Test _configure_data_path populates S3 storage options."""
        test_settings = Settings()
        test_settings.AWS_ACCESS_KEY_ID = "test-key"
        test_settings.AWS_SECRET_ACCESS_KEY = "test-secret"  # noqa: S105
        test_settings._configure_data_path()

        assert test_settings.S3_STORAGE_OPTIONS["AWS_ACCESS_KEY_ID"] == "test-key"
        assert test_settings.S3_STORAGE_OPTIONS["AWS_SECRET_ACCESS_KEY"] == "test-secret"  # noqa: S105
        assert test_settings.S3_STORAGE_OPTIONS["AWS_ALLOW_HTTP"] == "true"

    def test_configure_data_path_populates_iceberg_options(self):
        """Test _configure_data_path populates Iceberg storage options."""
        test_settings = Settings()
        test_settings.AWS_ACCESS_KEY_ID = "test-key"
        test_settings.AWS_SECRET_ACCESS_KEY = "test-secret"  # noqa: S105
        test_settings.AWS_ENDPOINT_URL = "http://minio:9000"
        test_settings.WAREHOUSE_BUCKET = "s3a://warehouse/"
        test_settings._configure_data_path()

        assert test_settings.ICEBERG_STORAGE_OPTIONS["s3.access-key-id"] == "test-key"
        assert test_settings.ICEBERG_STORAGE_OPTIONS["s3.secret-access-key"] == "test-secret"  # noqa: S105
        assert test_settings.ICEBERG_STORAGE_OPTIONS["s3.endpoint"] == "http://minio:9000"
        # s3a:// should be converted to s3://
        assert test_settings.ICEBERG_STORAGE_OPTIONS["warehouse"] == "s3://warehouse/"


class TestGetSettings:
    """Tests for the get_settings function."""

    def test_get_settings_returns_settings_instance(self):
        """Test get_settings returns a Settings instance."""
        # Clear cache first
        get_settings.cache_clear()
        settings = get_settings()
        assert isinstance(settings, Settings)

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
        # After reload, get_settings should return the new instance
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
