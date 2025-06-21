"""Source module for the package."""

from poor_man_lakehouse.config import settings as settings
from poor_man_lakehouse.spark.builder import (
    DeltaUnityCatalogSparkBuilder as DeltaUnityCatalogSparkBuilder,
)
from poor_man_lakehouse.spark.builder import (
    IcebergNessieSparkBuilder as IcebergNessieSparkBuilder,
)
from poor_man_lakehouse.spark.builder import retrieve_current_spark_session

spark = retrieve_current_spark_session()
