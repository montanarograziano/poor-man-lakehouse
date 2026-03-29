import os
import sys
from functools import cache
from pathlib import Path

from loguru import logger
from pydantic import computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SettingsError(Exception):
    """Raised when settings configuration fails."""


class Settings(BaseSettings):
    """Settings class for application settings and secrets management.

    Docs: https://docs.pydantic.dev/latest/concepts/pydantic_settings/
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application Path
    APP_NAME: str = "Poor Man Lakehouse"
    PROJECT_NAME: str = "poor-man-lakehouse"
    REPO_PATH: str = str(Path.cwd())

    # Logger
    LOG_VERBOSITY: str = "DEBUG"
    LOG_ROTATION_SIZE: str = "100MB"
    LOG_RETENTION: str = "30 days"
    LOG_FOLDER: str = ""
    LOG_FILE_NAME: str = "{time:D-M-YY}.log"

    # AWS Credentials
    AWS_DEFAULT_REGION: str = "eu-central-1"
    AWS_REGION: str = ""
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_ENDPOINT_URL: str = "http://minio:9000"
    MINIO_ENDPOINT: str = "http://minio:9000"
    AWS_SESSION_TOKEN: str = ""  # not necessary locally
    S3_STORAGE_OPTIONS: dict = {}
    ICEBERG_STORAGE_OPTIONS: dict = {}

    # Postgres credentials
    POSTGRES_HOST: str = "localhost"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = "lakehouse_db"

    # Catalog settings
    CATALOG: str = "nessie"  # Options: "unity_catalog", "nessie", "lakekeeper", "postgres"
    CATALOG_URI: str = "http://localhost:8080"
    CATALOG_NAME: str = "nessie"
    CATALOG_DEFAULT_SCHEMA: str = "default"

    # Nessie Configuration
    NESSIE_NATIVE_URI: str = "http://nessie:19120/api/v2"
    NESSIE_REST_URI: str = "http://nessie:19120/iceberg"

    # LakeKeeper Configuration
    LAKEKEEPER_SERVER_URI: str = "http://lakekeeper:8181/catalog"
    LAKEKEEPER_WAREHOUSE: str = "warehouse"

    # Unity Catalog Configuration
    UNITY_CATALOG_URI: str = "http://unity_catalog:8080/"

    # Dremio settings
    DREMIO_SERVER_URI: str = "http://dremio:9047"
    ARROW_ENDPOINT: str = "grpc://dremio:32010"
    DREMIO_USERNAME: str = "dremio"
    DREMIO_ROOT_PASSWORD: str = ""
    DREMIO_ROOT_EMAIL: str = "admin@example.com"

    # Spark settings
    SPARK_MASTER: str = "spark://localhost:7077"
    SPARK_DRIVER_HOST: str = "172.18.0.1"
    SPARK_DRIVER_PORT: int = 7001
    SPARK_DRIVER_BLOCK_MANAGER_PORT: int = 7002

    # AWS Path
    BUCKET_NAME: str = "warehouse"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def WAREHOUSE_BUCKET(self) -> str:
        """Compute warehouse bucket URI from BUCKET_NAME."""
        return f"s3://{self.BUCKET_NAME}/"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def SETTINGS_PATH(self) -> str:
        """Compute settings path from REPO_PATH."""
        return os.path.join(self.REPO_PATH, "settings")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def LOG_FILE_PATH(self) -> str:
        """Compute log file path from LOG_FOLDER and LOG_FILE_NAME."""
        return os.path.join(self.log_folder_resolved, self.LOG_FILE_NAME)

    @property
    def log_folder_resolved(self) -> str:
        """Resolve log folder, defaulting to REPO_PATH/logs."""
        return self.LOG_FOLDER or os.path.join(self.REPO_PATH, "logs")

    @model_validator(mode="after")
    def _initialize(self) -> "Settings":
        """Configure storage options after all fields are set."""
        self._configure_data_path()
        return self

    def _configure_data_path(self) -> None:
        """Configure S3 and Iceberg storage options."""
        self.S3_STORAGE_OPTIONS = {
            "AWS_ACCESS_KEY_ID": self.AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": self.AWS_SECRET_ACCESS_KEY,
            "AWS_REGION": self.AWS_DEFAULT_REGION,
            "AWS_ENDPOINT_URL": self.AWS_ENDPOINT_URL,
            "AWS_ALLOW_HTTP": "true",
            "allow_http": "true",
            "aws_conditional_put": "etag",
        }

        self.ICEBERG_STORAGE_OPTIONS = {
            "s3.endpoint": self.AWS_ENDPOINT_URL,
            "s3.access-key-id": self.AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": self.AWS_SECRET_ACCESS_KEY,
            "s3.region": self.AWS_DEFAULT_REGION,
            "warehouse": self.LAKEKEEPER_WAREHOUSE
            if self.CATALOG == "lakekeeper"
            else self.WAREHOUSE_BUCKET.replace("s3a://", "s3://"),
        }

    def _setup_logger(self) -> None:
        """Configure loguru logger with stderr and file handlers."""
        logger.remove()
        log_file_path = os.path.join(self.log_folder_resolved, self.LOG_FILE_NAME)
        logger.add(
            sink=sys.stderr,
            colorize=True,
            level=self.LOG_VERBOSITY,
            serialize=False,
            catch=True,
            backtrace=False,
            diagnose=False,
        )
        logger.add(
            sink=log_file_path,
            rotation=self.LOG_ROTATION_SIZE,
            retention=self.LOG_RETENTION,
            colorize=True,
            level=self.LOG_VERBOSITY,
            serialize=False,
            catch=True,
            backtrace=False,
            diagnose=False,
            encoding="utf8",
        )


@cache
def get_settings() -> Settings:
    """Generate and get the settings.

    Returns:
        Configured Settings instance.

    Raises:
        SettingsError: If settings initialization fails.
    """
    try:
        settings = Settings()
        settings._setup_logger()
        return settings

    except Exception as e:
        logger.error(f"Error: impossible to get the settings: {e}")
        raise SettingsError(f"Error importing settings: {e}") from e


def reload_settings() -> Settings:
    """Reload base settings."""
    get_settings.cache_clear()
    return get_settings()


def require_catalog(expected: str, *, connector_name: str, current_settings: Settings | None = None) -> None:
    """Validate that the active catalog matches the expected one.

    Args:
        expected: The required catalog name.
        connector_name: Name of the connector for error messages.
        current_settings: Settings instance to check. Defaults to the global singleton.

    Raises:
        ValueError: If the active catalog doesn't match.
    """
    s = current_settings or get_settings()
    if s.CATALOG.lower() != expected.lower():
        raise ValueError(
            f"{connector_name} requires '{expected}' catalog, "
            f"but CATALOG='{s.CATALOG}'. Set CATALOG={expected} in your environment."
        )


# default settings with initialization
settings = get_settings()
