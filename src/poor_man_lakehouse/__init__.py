"""Source module for the package."""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.poor_man_lakehouse.config import settings

extra_packages = [
    "org.apache.hadoop:hadoop-aws:3.4.0",
    "io.unitycatalog:unitycatalog-spark_2.13:0.3.0",
]
builder = (
    SparkSession.builder.appName("Poor Man Lakehouse")
    .master(settings.SPARK_MASTER)
    .config(
        "spark.sql.catalog.spark_catalog",
        "io.unitycatalog.spark.UCSingleCatalog",
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("fs.s3a.path.style.access", "true")
    .config(
        "spark.hadoop.fs.s3a.access.key",
        settings.S3_STORAGE_OPTIONS["AWS_ACCESS_KEY_ID"],
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        settings.S3_STORAGE_OPTIONS["AWS_SECRET_ACCESS_KEY"],
    )
    .config("spark.hadoop.fs.s3a.endpoint", settings.S3_STORAGE_OPTIONS["AWS_ENDPOINT"])
    .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog")
    .config("spark.sql.catalog.unity.uri", settings.CATALOG_URI)
    .config("spark.sql.defaultCatalog", settings.CATALOG_NAME)
)
spark = configure_spark_with_delta_pip(
    builder, extra_packages=extra_packages
).getOrCreate()
