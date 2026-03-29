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

::: poor_man_lakehouse.config.require_catalog

---

## Connectors

### IbisConnection

::: poor_man_lakehouse.ibis_connector.builder.IbisConnection
    options:
      members:
        - __init__
        - get_connection
        - read_table
        - write_table
        - create_table
        - sql
        - list_tables
        - set_current_database
        - close

### PyIcebergClient

::: poor_man_lakehouse.pyiceberg_connector.client.PyIcebergClient
    options:
      members:
        - __init__
        - list_namespaces
        - list_tables
        - load_table
        - table_schema
        - snapshot_history
        - scan_to_polars
        - scan_to_arrow

### CatalogBrowser

::: poor_man_lakehouse.catalog_browser.CatalogBrowser
    options:
      members:
        - __init__
        - list_namespaces
        - list_tables
        - get_table_schema
        - get_snapshot_count

### PolarsClient

::: poor_man_lakehouse.polars_connector.client.PolarsClient
    options:
      members:
        - __init__
        - list_catalogs
        - list_namespaces
        - list_tables
        - scan_table
        - sql
        - show_catalog_tree
        - describe_table
        - load_all_tables
        - to_duckdb
        - clear_cache

### DremioConnection

::: poor_man_lakehouse.dremio_connector.builder.DremioConnection
    options:
      members:
        - __init__
        - to_polars
        - to_duckdb
        - to_pandas
        - to_arrow
        - set_catalog
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
