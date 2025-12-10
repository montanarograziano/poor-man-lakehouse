from abc import ABC, abstractmethod

import pyspark
from delta import configure_spark_with_delta_pip
from loguru import logger
from pyspark.sql import SparkSession

from poor_man_lakehouse.config import settings

SCALA_VERSION = "2.13"


class SparkBuilder(ABC):
    root_builder = (
        SparkSession.Builder()
        .appName("Poor Man Lakehouse")
        .master(settings.SPARK_MASTER)
    )

    @abstractmethod
    def get_spark_session(self) -> SparkSession:
        """Abstract method to get a configured Spark session."""
        pass


class DeltaUnityCatalogSparkBuilder(SparkBuilder):
    """Builder for Spark session with Delta Lake and Unity Catalog support."""

    extra_packages = [
        "org.apache.hadoop:hadoop-aws:3.4.0",
        f"io.unitycatalog:unitycatalog-spark_{SCALA_VERSION}:0.3.0",
    ]

    def get_spark_session(self) -> SparkSession:
        return configure_spark_with_delta_pip(
            self.root_builder, extra_packages=self.extra_packages
        ).getOrCreate()


class IcebergNessieSparkBuilder(SparkBuilder):
    """Builder for Spark session with Iceberg and Nessie support."""

    def get_spark_session(self) -> SparkSession:
        extra_packages = [
            f"org.apache.iceberg:iceberg-spark-runtime-3.5_{SCALA_VERSION}:1.9.2",
            f"org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_{SCALA_VERSION}:0.103.3",
            "org.apache.iceberg:iceberg-aws-bundle:1.9.2",
        ]
        extra_jars = ",".join(extra_packages)

        spark = (
            self.root_builder.config("spark.jars.packages", extra_jars)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
            )
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
            .config(
                "spark.sql.catalog.nessie.uri", settings.NESSIE_PYICEBERG_SERVER_URI
            )
            .config("spark.sql.catalog.nessie.type", "rest")
            .config("spark.sql.catalog.nessie.ref", "main")
            .config("spark.sql.catalog.nessie.authentication.type", "NONE")
            .config("spark.sql.catalog.nessie.warehouse", settings.WAREHOUSE_BUCKET)
            .config("spark.sql.catalog.nessie.s3.endpoint", settings.AWS_ENDPOINT_URL)
            .config(
                "spark.sql.catalog.nessie.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config("spark.sql.defaultCatalog", "nessie")
            .config("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.endpoint.region", settings.AWS_DEFAULT_REGION)
            .config("spark.hadoop.fs.s3a.endpoint", settings.AWS_ENDPOINT_URL)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "1000")
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )
        spark.catalog.setCurrentCatalog("nessie")

        if not spark.catalog.databaseExists("nessie.default"):
            spark.sql("CREATE DATABASE nessie.default")
        spark.catalog.setCurrentDatabase("default")
        return spark


class IcebergLakeKeeperSparkBuilder(SparkBuilder):
    """Builder for Spark session with Iceberg and LakeKeeper support."""

    CATALOG_URL = "http://lakekeeper:8181/catalog"
    KEYCLOAK_TOKEN_ENDPOINT = (
        "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
    )
    WAREHOUSE = "warehouse"

    CLIENT_ID = "spark"
    CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"

    SPARK_VERSION = pyspark.__version__
    SPARK_MINOR_VERSION = ".".join(SPARK_VERSION.split(".")[:2])
    ICEBERG_VERSION = "1.7.0"

    def get_spark_session(self) -> SparkSession:
        extra_packages = [
            f"org.apache.iceberg:iceberg-spark-runtime-{self.SPARK_MINOR_VERSION}_{SCALA_VERSION}:1.9.1",
            "org.apache.iceberg:iceberg-aws-bundle:1.9.1",
            "org.apache.hadoop:hadoop-aws:3.4.0",
            "software.amazon.awssdk:bundle:2.31.68",
            "software.amazon.awssdk:url-connection-client:2.31.68",
        ]
        conf = (
            pyspark.SparkConf()
            .set(
                "spark.jars.packages",
                ",".join(extra_packages),
            )
            # SQL Extensions
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            # Configuring Catalog
            .set(
                "spark.sql.catalog.lakekeeper", "org.apache.iceberg.spark.SparkCatalog"
            )
            .set("spark.sql.catalog.lakekeeper.uri", settings.LAKEKEEPER_SERVER_URI)
            # .set("spark.sql.catalog.lakekeeper.credential", f"{self.CLIENT_ID}:{self.CLIENT_SECRET}")
            .set("spark.sql.catalog.nessie.authentication.type", "NONE")
            .set(
                "spark.sql.catalog.l.catalog-impl",
                "org.apache.iceberg.nessie.NessieCatalog",
            )
            .set("spark.sql.catalog.nessie.s3.endpoint", settings.AWS_ENDPOINT_URL)
            .set("spark.sql.catalog.nessie.warehouse", settings.WAREHOUSE_BUCKET)
            .set(
                "spark.sql.catalog.nessie.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .set("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID)
            .set("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)
            .set("spark.hadoop.fs.s3a.endpoint", settings.AWS_ENDPOINT_URL)
        )
        spark = self.root_builder.config(conf=conf).getOrCreate()
        if not spark.catalog.databaseExists("default"):
            spark.sql("CREATE DATABASE default")
        spark.catalog.setCurrentDatabase("default")
        return spark


def retrieve_current_spark_session() -> SparkSession:
    """Retrieve the current Spark session, creating it if it doesn't exist."""
    logger.debug(f"Setting up Spark session with catalog: {settings.CATALOG}")
    if settings.CATALOG == "unity_catalog":
        return DeltaUnityCatalogSparkBuilder().get_spark_session()
    elif settings.CATALOG == "nessie":
        return IcebergNessieSparkBuilder().get_spark_session()
    elif settings.CATALOG == "lake_keeper":
        return IcebergLakeKeeperSparkBuilder().get_spark_session()
    else:
        raise ValueError(f"Unsupported catalog: {settings.CATALOG}")
