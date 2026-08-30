"""Poor Man Lakehouse - Multi-engine data lakehouse connectors."""

from poor_man_lakehouse.catalog import LakehouseCatalogType, get_catalog
from poor_man_lakehouse.config import Settings, get_settings, reload_settings, settings
from poor_man_lakehouse.lakehouse import LakehouseConnection
from poor_man_lakehouse.maintenance import (
    ExpireSnapshotsPlan,
    MaintenanceResult,
    TableHealth,
    TableMaintenance,
)

__all__ = [
    # Catalog
    "LakehouseCatalogType",
    "get_catalog",
    # Config
    "Settings",
    "get_settings",
    "reload_settings",
    "settings",
    # Connectors
    "LakehouseConnection",
    # Maintenance
    "ExpireSnapshotsPlan",
    "MaintenanceResult",
    "TableHealth",
    "TableMaintenance",
    # Spark (lazy — avoids eager PySpark/Delta import)
    "CatalogType",
    "get_spark_builder",
    "retrieve_current_spark_session",
]

# Lazy imports for Spark symbols to avoid eagerly loading PySpark/Delta.
_SPARK_IMPORTS = {"CatalogType", "get_spark_builder", "retrieve_current_spark_session"}


def __getattr__(name: str) -> object:
    if name in _SPARK_IMPORTS:
        from poor_man_lakehouse.spark_connector import (
            CatalogType,
            get_spark_builder,
            retrieve_current_spark_session,
        )

        _spark_symbols = {
            "CatalogType": CatalogType,
            "get_spark_builder": get_spark_builder,
            "retrieve_current_spark_session": retrieve_current_spark_session,
        }
        # Cache in module globals so __getattr__ is not called again
        globals().update(_spark_symbols)
        return _spark_symbols[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
