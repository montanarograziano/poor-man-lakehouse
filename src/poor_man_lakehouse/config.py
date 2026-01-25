import os
import sys
from functools import cache

from loguru import logger
from pydantic_settings import BaseSettings, SettingsConfigDict


class SettingsError(Exception):
    """Raised when settings configuration fails."""


class Settings(BaseSettings):
    """Settings class for application settings and secrets management.

    Official documentation on pydantic settings management.

    - https://pydantic-docs.helpmanual.io/usage/settings/.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application Path
    APP_NAME: str = "Poor Man Lakehouse"
    PROJECT_NAME: str = "poor-man-lakehouse"
    REPO_PATH: str = os.path.abspath(".")
    SETTINGS_PATH: str = os.path.join(REPO_PATH, "settings")

    # Logger
    LOG_VERBOSITY: str = "DEBUG"
    LOG_ROTATION_SIZE: str = "100MB"
    LOG_RETENTION: str = "30 days"
    LOG_FOLDER: str = os.path.join(REPO_PATH, "logs")
    LOG_FILE_NAME: str = "{time:D-M-YY}.log"
    LOG_FILE_PATH: str = os.path.join(LOG_FOLDER, LOG_FILE_NAME)

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
    CATALOG: str = "nessie"  # Options: "unity_catalog", "nessie"
    CATALOG_URI: str = "http://localhost:8080"
    CATALOG_NAME: str = "nessie"
    CATALOG_DEFAULT_SCHEMA: str = "default"

    # Nessie Configuration
    NESSIE_NATIVE_URI: str = "http://nessie:19120/api/v2"
    NESSIE_REST_URI: str = "http://nessie:19120/iceberg"

    # LakeKeeper Configuration
    LAKEKEEPER_SERVER_URI: str = "http://lakekeeper:8181"

    # Unity Catalog Configuration
    UNITY_CATALOG_URI: str = "http://unity_catalog:8080/"

    # Dremio settings
    DREMIO_SERVER_URI: str = "http://dremio:9047"
    ARROW_ENDPOINT: str = "grpc://dremio:32010"
    DREMIO_USERNAME: str = "dremio"
    DREMIO_ROOT_PASSWORD: str = ""
    DREMIO_ROOT_EMAIL: str = "admin@example.com"

    # Spark settings
    SPARK_MASTER: str = "spark://spark-master:7077"

    # AWS Path
    BUCKET_NAME: str = "warehouse"
    WAREHOUSE_BUCKET: str = f"s3://{BUCKET_NAME}/"

    def _configure_data_path(self):
        """Configure S3 storage options."""
        self.S3_STORAGE_OPTIONS = {
            "AWS_ACCESS_KEY_ID": self.AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": self.AWS_SECRET_ACCESS_KEY,
            "AWS_SESSION_TOKEN": self.AWS_SESSION_TOKEN,
            "AWS_REGION": self.AWS_DEFAULT_REGION,
            "AWS_DEFAULT_REGION": self.AWS_DEFAULT_REGION,
            "AWS_ENDPOINT_URL": self.AWS_ENDPOINT_URL,
            "AWS_ALLOW_HTTP": "true",
            "aws_conditional_put": "etag",
        }

        self.ICEBERG_STORAGE_OPTIONS = {
            "s3.endpoint": self.AWS_ENDPOINT_URL,
            "s3.access-key-id": self.AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": self.AWS_SECRET_ACCESS_KEY,
            "s3.region": self.AWS_DEFAULT_REGION,
            "warehouse": self.WAREHOUSE_BUCKET.replace("s3a://", "s3://"),
        }

    def _setup_logger(self) -> None:
        """Configure loguru logger with stderr and file handlers."""
        logger.remove()  # to remove previous handlers and reset
        log_file_path = os.path.join(self.LOG_FOLDER, self.LOG_FILE_NAME)
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
        settings._configure_data_path()
        settings._setup_logger()
        return settings

    except Exception as e:
        logger.error(f"Error: impossible to get the settings: {e}")
        raise SettingsError(f"Error importing settings: {e}") from e


def reload_settings() -> Settings:
    """Reload base settings."""
    get_settings.cache_clear()
    return get_settings()


# default settings with initialization
settings = get_settings()
