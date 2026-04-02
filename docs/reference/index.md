# API Reference

Auto-generated documentation from source code docstrings.

## Configuration

::: poor_man_lakehouse.config.Settings
    options:
      show_source: false
      members:
        - WAREHOUSE_BUCKET
        - SETTINGS_PATH
        - LOG_FILE_PATH

::: poor_man_lakehouse.config.get_settings

::: poor_man_lakehouse.config.reload_settings

---

## Catalog Factory

::: poor_man_lakehouse.catalog.get_catalog

::: poor_man_lakehouse.catalog.LakehouseCatalogType

---

## Connectors

### LakehouseConnection

::: poor_man_lakehouse.lakehouse.LakehouseConnection
    options:
      members:
        - __init__
        - list_namespaces
        - list_tables
        - load_table
        - table_schema
        - snapshot_history
        - scan_polars
        - scan_arrow
        - duckdb_connection
        - ibis_duckdb
        - ibis_polars
        - ibis_pyspark
        - sql
        - write_table
        - create_table
        - close

---

## Spark

### SparkBuilder

::: poor_man_lakehouse.spark_connector.builder.SparkBuilder
    options:
      show_source: false
      members:
        - get_spark_session
        - catalog_name

::: poor_man_lakehouse.spark_connector.builder.CatalogType

::: poor_man_lakehouse.spark_connector.builder.get_spark_builder

::: poor_man_lakehouse.spark_connector.builder.retrieve_current_spark_session
