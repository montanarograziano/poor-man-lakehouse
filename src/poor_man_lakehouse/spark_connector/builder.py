"""Spark session builders for different catalog implementations.

All builders share common JARs (Iceberg, Delta, Hadoop-AWS) for Spark 4.0.1 compatibility.
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

# Common packages for all Spark 4.0.1 implementations
# These provide Iceberg, Delta (via configure_spark_with_delta_pip), and S3 support
COMMON_PACKAGES: list[str] = [
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MAJOR_MINOR}_{SCALA_VERSION}:1.10.1",
    "org.apache.iceberg:iceberg-aws-bundle:1.10.1",
    "org.apache.hadoop:hadoop-aws:3.4.1",
    "org.postgresql:postgresql:42.7.10",
    f"org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_{SCALA_VERSION}:0.106.0",
]


class CatalogType(str, Enum):
    """Supported catalog types."""

    POSTGRES = "postgres"
    NESSIE = "nessie"
    LAKEKEEPER = "lakekeeper"
    GLUE = "glue"


class SparkBuilder(ABC):
    """Abstract base class for Spark session builders.

    Provides common configuration for S3/Minio access and shared packages.
    Subclasses only need to implement catalog-specific configuration.
    """

    # SQL extensions for Iceberg and Delta support
    ICEBERG_EXTENSIONS: ClassVar[str] = (
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
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
        """Create a fresh SparkSession builder with common configuration.

        Driver host/port are only set for remote Spark clusters (spark://...).
        In local[*] mode they are unnecessary and can cause connection errors.
        """
        builder = SparkSession.builder.appName(self._app_name).master(settings.SPARK_MASTER)

        # Only configure driver networking for remote clusters
        if not settings.SPARK_MASTER.startswith("local"):
            builder = (
                builder.config("spark.driver.host", settings.SPARK_DRIVER_HOST)
                .config("spark.driver.bindAddress", "0.0.0.0")  # noqa: S104
                .config("spark.driver.port", settings.SPARK_DRIVER_PORT)
                .config("spark.driver.blockManager.port", settings.SPARK_DRIVER_BLOCK_MANAGER_PORT)
            )

        return builder

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
        # DeltaCatalog on spark_catalog enables path-based Delta operations
        # This is separate from the named Iceberg catalog and they coexist
        builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
            .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
            .config(f"spark.sql.catalog.{catalog}.client-api-version", "2")
            .config(f"spark.sql.catalog.{catalog}.uri", settings.NESSIE_NATIVE_URI)
            .config(f"spark.sql.catalog.{catalog}.ref", "main")
            .config(f"spark.sql.catalog.{catalog}.authentication.type", "NONE")
            .config(f"spark.sql.catalog.{catalog}.warehouse", settings.WAREHOUSE_BUCKET)
            .config(f"spark.sql.catalog.{catalog}.s3.endpoint", settings.AWS_ENDPOINT_URL)
            .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
            .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)
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
    Lakekeeper vends temporary S3 credentials, so static S3 keys are NOT
    set in Spark config — only the S3A endpoint and path-style access.

    Based on official Lakekeeper Spark documentation:
    https://docs.lakekeeper.io/docs/nightly/engines/#spark
    """

    @property
    def catalog_name(self) -> str:
        """Return the catalog name for this builder.

        Lakekeeper uses 'lakekeeper' as the catalog name regardless of settings.
        """
        return "lakekeeper"

    def _configure_common(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        """Apply common configuration without static S3 credentials.

        Lakekeeper vends temporary credentials per-table via the Iceberg REST
        protocol. Static fs.s3a.access.key/secret.key must NOT be set or they
        will conflict with vended credentials.
        """
        extensions = f"{self.ICEBERG_EXTENSIONS},{self.DELTA_EXTENSIONS}"
        return (
            builder.config("spark.sql.extensions", extensions)
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", settings.AWS_ENDPOINT_URL)
            .config("spark.hadoop.fs.s3a.endpoint.region", settings.AWS_DEFAULT_REGION)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    def _configure_catalog(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        catalog = self.catalog_name

        return (
            builder.config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            .config(f"spark.sql.catalog.{catalog}.uri", settings.LAKEKEEPER_SERVER_URI)
            .config(f"spark.sql.catalog.{catalog}.warehouse", settings.LAKEKEEPER_WAREHOUSE)
            .config(f"spark.sql.catalog.{catalog}.header.X-Iceberg-Access-Delegation", "vended-credentials")
            .config("spark.sql.defaultCatalog", catalog)
        )

    def get_spark_session(self) -> SparkSession:
        """Get a Spark session configured for the Lakekeeper catalog."""
        spark = super().get_spark_session()
        self._ensure_default_database(spark, self.catalog_name)
        return spark


class GlueCatalogSparkBuilder(SparkBuilder):
    """Builder for Spark session with AWS Glue Catalog for Iceberg tables.

    Uses AWS Glue as the Iceberg catalog backend. Credentials are resolved via
    the AWS default credential chain (environment variables, ~/.aws/credentials,
    IAM roles) — no static S3 keys are injected into Spark config.

    Credential resolution order:
        1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        2. AWS credentials file (~/.aws/credentials)
        3. IAM instance profile / role (EC2, ECS, Lambda)
        4. AWS SSO / STS assume-role

    Requires:
        - Valid AWS credentials with Glue and S3 permissions
        - An S3 bucket for the warehouse (BUCKET_NAME setting)
        - AWS_DEFAULT_REGION set to the Glue catalog's region
    """

    def _configure_common(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        """Apply common configuration without static S3 credentials.

        AWS Glue uses the default credential chain — static fs.s3a keys are not
        set so the AWS SDK can resolve credentials automatically from the
        environment, credentials file, or IAM role.
        """
        extensions = f"{self.ICEBERG_EXTENSIONS},{self.DELTA_EXTENSIONS}"
        return (
            builder.config("spark.sql.extensions", extensions)
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint.region", settings.AWS_DEFAULT_REGION)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    def _configure_catalog(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        catalog = self.catalog_name

        builder = (
            builder.config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{catalog}.warehouse", settings.WAREHOUSE_BUCKET)
            .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.defaultCatalog", catalog)
        )

        if settings.GLUE_CATALOG_ID:
            builder = builder.config(f"spark.sql.catalog.{catalog}.glue.id", settings.GLUE_CATALOG_ID)

        return builder

    def get_spark_session(self) -> SparkSession:
        """Get a Spark session configured for AWS Glue Catalog."""
        spark = super().get_spark_session()
        self._ensure_default_database(spark, self.catalog_name)
        return spark


_CATALOG_BUILDERS: dict[CatalogType, type[SparkBuilder]] = {
    CatalogType.POSTGRES: PostgresCatalogSparkBuilder,
    CatalogType.NESSIE: NessieCatalogSparkBuilder,
    CatalogType.LAKEKEEPER: LakekeeperCatalogSparkBuilder,
    CatalogType.GLUE: GlueCatalogSparkBuilder,
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
