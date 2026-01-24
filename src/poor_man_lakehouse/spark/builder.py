"""Spark session builders for different catalog implementations.

All builders share common JARs (Iceberg, Delta, Hadoop-AWS) for Spark 4.0.0 compatibility.
Delta support is enabled via configure_spark_with_delta_pip for all builders, allowing
path-based Delta table access (e.g., spark.read.format("delta").load("s3a://...")).
The main difference between implementations is the catalog configuration for Iceberg tables.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import ClassVar

from delta import configure_spark_with_delta_pip
from loguru import logger
from pyspark.sql import SparkSession

from poor_man_lakehouse.config import settings

SCALA_VERSION = "2.13"
SPARK_MAJOR_MINOR = "4.0"

# Common packages for all Spark 4.0.0 implementations
# These provide Iceberg, Delta (via configure_spark_with_delta_pip), and S3 support
COMMON_PACKAGES: list[str] = [
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MAJOR_MINOR}_{SCALA_VERSION}:1.10.1",
    "org.apache.iceberg:iceberg-aws-bundle:1.10.1",
    "org.apache.hadoop:hadoop-aws:3.4.1",
    "org.postgresql:postgresql:42.7.3",
]


class CatalogType(str, Enum):
    """Supported catalog types."""

    POSTGRES = "postgres"
    UNITY_CATALOG = "unity_catalog"
    NESSIE = "nessie"
    LAKEKEEPER = "lakekeeper"


class SparkBuilder(ABC):
    """Abstract base class for Spark session builders.

    Provides common configuration for S3/Minio access and shared packages.
    Subclasses only need to implement catalog-specific configuration.
    """

    # SQL extensions for Iceberg and Delta support
    ICEBERG_EXTENSIONS: ClassVar[str] = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    DELTA_EXTENSIONS: ClassVar[str] = "io.delta.sql.DeltaSparkSessionExtension"

    @property
    def catalog_name(self) -> str:
        """Return the catalog name for this builder.

        Default implementation uses settings.CATALOG_NAME.
        Subclasses can override for custom catalog names.
        """
        return settings.CATALOG_NAME

    def __init__(self, app_name: str = "Poor Man Lakehouse") -> None:
        """Initialize the builder with application name.

        Args:
            app_name: Name of the Spark application.
        """
        self._app_name = app_name

    def _create_base_builder(self) -> SparkSession.Builder:
        """Create a fresh SparkSession builder with common configuration."""
        return SparkSession.builder.appName(self._app_name).master(settings.SPARK_MASTER)

    def _get_packages(self) -> list[str]:
        """Get the list of Maven packages to include.

        Subclasses can override to add additional packages.
        """
        return COMMON_PACKAGES.copy()

    def _configure_s3(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        """Configure S3/Minio access settings.

        Args:
            builder: The SparkSession builder to configure.

        Returns:
            The configured builder.
        """
        return (
            builder.config("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.endpoint.region", settings.AWS_DEFAULT_REGION)
            .config("spark.hadoop.fs.s3a.endpoint", settings.AWS_ENDPOINT_URL)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    def _configure_common(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        """Apply common configuration to the builder.

        Args:
            builder: The SparkSession builder to configure.

        Returns:
            The configured builder with extensions and S3 settings.
            Note: Packages are handled via configure_spark_with_delta_pip in get_spark_session.
        """
        extensions = f"{self.ICEBERG_EXTENSIONS},{self.DELTA_EXTENSIONS}"
        builder = builder.config("spark.sql.extensions", extensions)
        return self._configure_s3(builder)

    @abstractmethod
    def _configure_catalog(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        """Configure catalog-specific settings.

        Args:
            builder: The SparkSession builder to configure.

        Returns:
            The configured builder with catalog settings.
        """

    def _ensure_default_database(self, spark: SparkSession, catalog_name: str) -> None:
        """Ensure the default database exists in the catalog.

        Args:
            spark: The active SparkSession.
            catalog_name: Name of the catalog to use.
        """
        spark.catalog.setCurrentCatalog(catalog_name)
        if not spark.catalog.databaseExists(f"{catalog_name}.default"):
            spark.sql(f"CREATE DATABASE {catalog_name}.default")
        spark.catalog.setCurrentDatabase("default")

    def get_spark_session(self) -> SparkSession:
        """Build and return a configured Spark session with Iceberg and Delta support.

        Returns:
            A configured SparkSession instance with both Iceberg catalog access
            and Delta Lake path-based access enabled.
        """
        builder = self._create_base_builder()
        builder = self._configure_common(builder)
        builder = self._configure_catalog(builder)
        extra_packages = self._get_packages()
        return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()


class PostgresCatalogSparkBuilder(SparkBuilder):
    """Builder for Spark session with PostgreSQL-backed Iceberg catalog.

    Uses PostgreSQL as the Iceberg catalog backend via JDBC.
    This is the tested and recommended implementation.
    """

    def _configure_catalog(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        catalog = self.catalog_name
        jdbc_uri = f"jdbc:postgresql://{settings.POSTGRES_HOST}/{settings.POSTGRES_DB}"

        return (
            builder.config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.type", "jdbc")
            .config(f"spark.sql.catalog.{catalog}.uri", jdbc_uri)
            .config(f"spark.sql.catalog.{catalog}.jdbc.user", settings.POSTGRES_USER)
            .config(f"spark.sql.catalog.{catalog}.jdbc.password", settings.POSTGRES_PASSWORD)
            .config(f"spark.sql.catalog.{catalog}.warehouse", settings.WAREHOUSE_BUCKET)
            .config("spark.sql.defaultCatalog", catalog)
        )

    def get_spark_session(self) -> SparkSession:
        """Get a Spark session configured for PostgreSQL-backed Iceberg catalog."""
        spark = super().get_spark_session()
        self._ensure_default_database(spark, self.catalog_name)
        return spark


class DeltaUnityCatalogSparkBuilder(SparkBuilder):
    """Builder for Spark session with Delta Lake and Unity Catalog support.

    Uses configure_spark_with_delta_pip for Delta Lake JAR management.
    """

    UNITY_CATALOG_PACKAGE: ClassVar[str] = f"io.unitycatalog:unitycatalog-spark_{SCALA_VERSION}:0.3.0"

    def _get_packages(self) -> list[str]:
        packages = super()._get_packages()
        packages.append(self.UNITY_CATALOG_PACKAGE)
        return packages

    def _configure_catalog(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        return builder.config("spark.sql.defaultCatalog", settings.CATALOG)

    def get_spark_session(self) -> SparkSession:
        """Get a Spark session configured for Delta Lake with Unity Catalog."""
        return super().get_spark_session()


class NessieCatalogSparkBuilder(SparkBuilder):
    """Builder for Spark session with Nessie catalog.

    Uses Nessie's REST catalog interface for Iceberg tables.

    Note: Nessie Spark extensions for Spark 4.0 may have limited availability.
    Check https://projectnessie.org for the latest compatible versions.
    """

    def _configure_catalog(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        catalog = self.catalog_name

        return (
            builder.config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.type", "rest")
            .config(f"spark.sql.catalog.{catalog}.uri", settings.NESSIE_PYICEBERG_SERVER_URI)
            .config(f"spark.sql.catalog.{catalog}.ref", "main")
            .config(f"spark.sql.catalog.{catalog}.authentication.type", "NONE")
            .config(f"spark.sql.catalog.{catalog}.warehouse", settings.WAREHOUSE_BUCKET)
            .config(f"spark.sql.catalog.{catalog}.s3.endpoint", settings.AWS_ENDPOINT_URL)
            .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.defaultCatalog", catalog)
        )

    def get_spark_session(self) -> SparkSession:
        """Get a Spark session configured for the Nessie catalog."""
        spark = super().get_spark_session()
        self._ensure_default_database(spark, self.catalog_name)
        return spark


class LakekeeperCatalogSparkBuilder(SparkBuilder):
    """Builder for Spark session with Lakekeeper catalog.

    Uses Lakekeeper's REST catalog interface for Iceberg tables.
    """

    def _configure_catalog(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        catalog = self.catalog_name

        return (
            builder.config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.type", "rest")
            .config(f"spark.sql.catalog.{catalog}.uri", settings.LAKEKEEPER_SERVER_URI)
            .config(f"spark.sql.catalog.{catalog}.warehouse", settings.WAREHOUSE_BUCKET)
            .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"spark.sql.catalog.{catalog}.s3.endpoint", settings.AWS_ENDPOINT_URL)
            .config("spark.sql.defaultCatalog", catalog)
        )

    def get_spark_session(self) -> SparkSession:
        """Get a Spark session configured for the Lakekeeper catalog."""
        spark = super().get_spark_session()
        self._ensure_default_database(spark, self.catalog_name)
        return spark


_CATALOG_BUILDERS: dict[CatalogType, type[SparkBuilder]] = {
    CatalogType.POSTGRES: PostgresCatalogSparkBuilder,
    CatalogType.UNITY_CATALOG: DeltaUnityCatalogSparkBuilder,
    CatalogType.NESSIE: NessieCatalogSparkBuilder,
    CatalogType.LAKEKEEPER: LakekeeperCatalogSparkBuilder,
}


def get_spark_builder(catalog_type: CatalogType | str) -> SparkBuilder:
    """Get the appropriate Spark builder for the given catalog type.

    Args:
        catalog_type: The catalog type (enum or string).

    Returns:
        An instance of the appropriate SparkBuilder subclass.

    Raises:
        ValueError: If the catalog type is not supported.
    """
    if isinstance(catalog_type, str):
        try:
            catalog_type = CatalogType(catalog_type)
        except ValueError as e:
            supported = [c.value for c in CatalogType]
            raise ValueError(f"Unsupported catalog: {catalog_type}. Supported: {supported}") from e

    builder_class = _CATALOG_BUILDERS.get(catalog_type)
    if builder_class is None:
        supported = [c.value for c in CatalogType]
        raise ValueError(f"Unsupported catalog: {catalog_type}. Supported: {supported}")

    return builder_class()


def retrieve_current_spark_session() -> SparkSession:
    """Retrieve a Spark session configured for the current catalog setting.

    Uses the CATALOG setting from configuration to determine which
    catalog implementation to use.

    Returns:
        A configured SparkSession instance.

    Raises:
        ValueError: If the configured catalog is not supported.
    """
    logger.debug(f"Setting up Spark session with catalog: {settings.CATALOG}")
    builder = get_spark_builder(settings.CATALOG)
    return builder.get_spark_session()
