"""Shared pytest fixtures for poor-man-lakehouse tests."""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_settings():
    """Provide mock settings for unit tests.

    Patches the settings module to return controlled test values
    without requiring actual environment variables or .env file.
    """
    with patch("poor_man_lakehouse.config.Settings") as mock_settings_class:
        mock_instance = MagicMock()

        # Application settings
        mock_instance.APP_NAME = "Poor Man Lakehouse"
        mock_instance.PROJECT_NAME = "poor-man-lakehouse"
        mock_instance.REPO_PATH = "/tmp/test-repo"  # noqa: S108
        mock_instance.SETTINGS_PATH = "/tmp/test-repo/settings"  # noqa: S108

        # Logger settings
        mock_instance.LOG_VERBOSITY = "DEBUG"
        mock_instance.LOG_ROTATION_SIZE = "100MB"
        mock_instance.LOG_RETENTION = "30 days"
        mock_instance.LOG_FOLDER = "/tmp/test-repo/logs"  # noqa: S108
        mock_instance.LOG_FILE_NAME = "test.log"

        # AWS/S3 settings
        mock_instance.AWS_ACCESS_KEY_ID = "test-access-key"
        mock_instance.AWS_SECRET_ACCESS_KEY = "test-secret-key"  # noqa: S105
        mock_instance.AWS_ENDPOINT_URL = "http://localhost:9000"
        mock_instance.AWS_DEFAULT_REGION = "eu-central-1"
        mock_instance.AWS_REGION = "eu-central-1"
        mock_instance.AWS_SESSION_TOKEN = ""
        mock_instance.MINIO_ENDPOINT = "http://localhost:9000"
        mock_instance.BUCKET_NAME = "warehouse"
        mock_instance.WAREHOUSE_BUCKET = "s3a://warehouse/"

        # Storage options (populated by _configure_data_path)
        mock_instance.S3_STORAGE_OPTIONS = {
            "AWS_ACCESS_KEY_ID": "test-access-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret-key",
            "AWS_ENDPOINT_URL": "http://localhost:9000",
            "AWS_DEFAULT_REGION": "eu-central-1",
            "AWS_ALLOW_HTTP": "true",
        }
        mock_instance.ICEBERG_STORAGE_OPTIONS = {
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "test-access-key",
            "s3.secret-access-key": "test-secret-key",
            "s3.region": "eu-central-1",
            "warehouse": "s3://warehouse/",
        }

        # Postgres settings
        mock_instance.POSTGRES_HOST = "localhost"
        mock_instance.POSTGRES_USER = "postgres"
        mock_instance.POSTGRES_PASSWORD = "password"  # noqa: S105
        mock_instance.POSTGRES_DB = "lakehouse_db"

        # Catalog settings
        mock_instance.CATALOG = "nessie"
        mock_instance.CATALOG_URI = "http://localhost:8080"
        mock_instance.CATALOG_NAME = "nessie"
        mock_instance.CATALOG_DEFAULT_SCHEMA = "default"

        # Nessie settings
        mock_instance.NESSIE_SPARK_SERVER_URI = "http://localhost:19120/api/v1"
        mock_instance.NESSIE_DREMIO_SERVER_URI = "http://localhost:19120/api/v2"
        mock_instance.NESSIE_PYICEBERG_SERVER_URI = "http://localhost:19120/iceberg"

        # Lakekeeper settings
        mock_instance.LAKEKEEPER_SERVER_URI = "http://localhost:8181"

        # Dremio settings
        mock_instance.DREMIO_SERVER_URI = "http://localhost:9047"
        mock_instance.ARROW_ENDPOINT = "grpc://localhost:32010"
        mock_instance.DREMIO_USERNAME = "dremio"
        mock_instance.DREMIO_ROOT_PASSWORD = "password123"  # noqa: S105
        mock_instance.DREMIO_ROOT_EMAIL = "admin@example.com"

        # Spark settings
        mock_instance.SPARK_MASTER = "local[*]"

        mock_settings_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_spark_session():
    """Provide a mock SparkSession for unit tests."""
    mock_session = MagicMock()
    mock_session.catalog.databaseExists.return_value = False
    mock_session.catalog.setCurrentCatalog = MagicMock()
    mock_session.catalog.setCurrentDatabase = MagicMock()
    mock_session.sql = MagicMock()
    return mock_session


@pytest.fixture
def mock_spark_builder():
    """Provide a mock SparkSession.Builder for unit tests."""
    mock_builder = MagicMock()
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = MagicMock()
    return mock_builder
