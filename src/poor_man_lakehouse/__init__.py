"""Poor Man Lakehouse - Multi-engine data lakehouse connectors."""

from poor_man_lakehouse.config import Settings, get_settings, reload_settings, settings
from poor_man_lakehouse.dremio_connector import DremioConnection
from poor_man_lakehouse.ibis_connector import IbisConnection
from poor_man_lakehouse.polars_connector import PolarsClient, load_sql_magic
from poor_man_lakehouse.spark_connector import (
    CatalogType,
    get_spark_builder,
    retrieve_current_spark_session,
)

__all__ = [
    # Config
    "Settings",
    "get_settings",
    "reload_settings",
    "settings",
    # Connectors
    "DremioConnection",
    "IbisConnection",
    "PolarsClient",
    "load_sql_magic",
    # Spark
    "CatalogType",
    "get_spark_builder",
    "retrieve_current_spark_session",
]
