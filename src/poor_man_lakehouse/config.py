import os
import sys
from functools import cache

from loguru import logger
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Settings class for application settings and secrets management.

    Official documentation on pydantic settings management.

    - https://pydantic-docs.helpmanual.io/usage/settings/.
    """

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
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_ENDPOINT: str = "http://localhost:9000"
    MINIO_ENDPOINT: str = "http://minio:9000"
    AWS_SESSION_TOKEN: str = ""  # not necessary locally
    S3_STORAGE_OPTIONS: dict = {}

    # Catalog settings
    CATALOG: str = "nessie"  # Options: "unity_catalog", "nessie"
    CATALOG_URI: str = "http://localhost:8080"
    CATALOG_NAME: str = "unity"
    CATALOG_DEFAULT_SCHEMA: str = "default"

    # Nessie Configuration
    NESSIE_SPARK_SERVER_URI: str = "http://localhost:19120:/api/v1"
    NESSIE_DREMIO_SERVER_URI: str = "http://localhost:19120:/api/v2"

    # LakeKeeper Configuration
    LAKEKEEPER_SERVER_URI: str = "http://localhost:8181"

    # Dremio settings
    DREMIO_SERVER_URI: str = "http://dremio:9047"
    ARROW_ENDPOINT: str = "grpc://localhost:32010"
    DREMIO_USERNAME: str = "admin"
    DREMIO_PASSWORD: str = "cambiami01"

    # Spark settings
    SPARK_MASTER: str = "spark://localhost:7077"

    # AWS Path
    BUCKET_NAME: str = "test"
    WAREHOUSE_BUCKET: str = "s3a://warehouse/"

    def _configure_data_path(self):
        """Configure S3 storage options."""

        self.S3_STORAGE_OPTIONS = {
            "AWS_ACCESS_KEY_ID": self.AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": self.AWS_SECRET_ACCESS_KEY,
            "AWS_SESSION_TOKEN": self.AWS_SESSION_TOKEN,
            "AWS_REGION": self.AWS_DEFAULT_REGION,
            "AWS_DEFAULT_REGION": self.AWS_DEFAULT_REGION,
            "AWS_ENDPOINT": self.AWS_ENDPOINT,
        }

    def _setup_logger(self):
        logger.remove()  # to remove previous handlers and reset
        self.LOG_FILE_NAME: str = self.LOG_FILE_NAME
        self.LOG_FOLDER: str = self.LOG_FOLDER
        self.LOG_FILE_PATH: str = os.path.join(self.LOG_FOLDER, self.LOG_FILE_NAME)
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
            sink=self.LOG_FILE_PATH,
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
def get_settings(settings: Settings = Settings()) -> Settings:
    """Generate and get the settings."""
    try:
        settings._configure_data_path()
        settings._setup_logger()
        return settings  # noqa

    except Exception as message:
        logger.error(f"Error: impossible to get the settings: {message}")
        raise Exception(f"Error importing settings: {message}") from message


def reload_settings() -> Settings:
    """Reload base settings."""
    get_settings.cache_clear()
    return get_settings()


# default settings with initialization
settings = get_settings()
