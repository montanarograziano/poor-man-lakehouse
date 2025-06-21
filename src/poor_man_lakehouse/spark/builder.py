from abc import ABC, abstractmethod

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from poor_man_lakehouse.config import settings


class SparkBuilder(ABC):
    # Shared root_builder for all subclasses
    root_builder = SparkSession.builder.appName("Poor Man Lakehouse").master(
        settings.SPARK_MASTER
    )

    @abstractmethod
    def get_spark_session(self) -> SparkSession:
        """Abstract method to get a configured Spark session."""
        pass


class DeltaUnityCatalogSparkBuilder(SparkBuilder):
    """Builder for Spark session with Delta Lake and Unity Catalog support."""

    extra_packages = [
        "org.apache.hadoop:hadoop-aws:3.4.0",
        "io.unitycatalog:unitycatalog-spark_2.13:0.3.0",
    ]

    def get_spark_session(self) -> SparkSession:
        return configure_spark_with_delta_pip(
            self.root_builder, extra_packages=self.extra_packages
        ).getOrCreate()


class IcebergNessieSparkBuilder(SparkBuilder):
    """Builder for Spark session with Iceberg and Nessie support."""

    def get_spark_session(self) -> SparkSession:
        conf = (
            pyspark.SparkConf()
            .set(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3,software.amazon.awssdk:bundle:2.20.131,software.amazon.awssdk:url-connection-client:2.20.131",
            )
            # SQL Extensions
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
            )
            # Configuring Catalog
            .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.nessie.uri", settings.NESSIE_SERVER_URI)
            .set("spark.sql.catalog.nessie.ref", "main")
            .set("spark.sql.catalog.nessie.authentication.type", "NONE")
            .set(
                "spark.sql.catalog.nessie.catalog-impl",
                "org.apache.iceberg.nessie.NessieCatalog",
            )
            .set("spark.sql.catalog.nessie.s3.endpoint", settings.MINIO_ENDPOINT)
            .set("spark.sql.catalog.nessie.warehouse", settings.WAREHOUSE_BUCKET)
            .set(
                "spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
            )
        )

        return self.root_builder.config(conf=conf).getOrCreate()


def retrieve_current_spark_session() -> SparkSession:
    """Retrieve the current Spark session, creating it if it doesn't exist."""
    if settings.CATALOG == "unity_catalog":
        return DeltaUnityCatalogSparkBuilder().get_spark_session()
    elif settings.CATALOG == "nessie":
        return IcebergNessieSparkBuilder().get_spark_session()
    else:
        raise ValueError(f"Unsupported catalog: {settings.CATALOG}")
