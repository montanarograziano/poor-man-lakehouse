# Catalog Browser

`CatalogBrowser` provides a uniform interface for browsing Iceberg catalog metadata across different backends (Nessie, Lakekeeper) using PyIceberg's REST catalog support.

## Basic Usage

```python
from poor_man_lakehouse import CatalogBrowser

# Auto-resolves URI from CATALOG setting
browser = CatalogBrowser()

# List namespaces
namespaces = browser.list_namespaces()
# ['default', 'staging']

# List tables
tables = browser.list_tables("default")
# ['users', 'orders', 'events']

# Get table schema
schema = browser.get_table_schema("default", "users")
# [
#   {"field_id": 1, "name": "id", "type": "long", "required": True},
#   {"field_id": 2, "name": "name", "type": "string", "required": False},
# ]

# Get snapshot count
count = browser.get_snapshot_count("default", "users")
```

## Automatic URI Resolution

`CatalogBrowser` automatically resolves the REST catalog URI based on your `CATALOG` setting:

| `CATALOG` Setting | URI Used |
|------------------|----------|
| `lakekeeper` | `LAKEKEEPER_SERVER_URI` |
| `nessie` | `NESSIE_REST_URI` |
| Other | Falls back to `LAKEKEEPER_SERVER_URI` |

Override with a custom URI:

```python
browser = CatalogBrowser(
    catalog_uri="http://custom-catalog:8181/catalog",
    catalog_name="my-catalog",
)
```

## Difference from PyIcebergClient

`CatalogBrowser` is a thin browsing layer that returns simple Python types (strings, dicts). `PyIcebergClient` is a full-featured client that returns PyIceberg objects (Table, Schema) and supports scanning to Polars/Arrow.

| Feature | CatalogBrowser | PyIcebergClient |
|---------|---------------|-----------------|
| List namespaces | `list[str]` | `list[tuple[str, ...]]` |
| List tables | `list[str]` | `list[tuple[str, str]]` |
| Table schema | `list[dict]` | `list[dict]` |
| Load table object | No | Yes |
| Scan to Polars | No | Yes |
| Scan to Arrow | No | Yes |
| Snapshot history | Count only | Full history |
| Auto URI resolution | Yes | No |

Use `CatalogBrowser` for quick catalog exploration. Use `PyIcebergClient` when you need to work with table data.
