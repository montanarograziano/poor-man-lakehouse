# PyIceberg Client

`PyIcebergClient` provides direct access to Iceberg table operations without requiring a JVM. It works with any REST-compatible catalog (Lakekeeper, Nessie).

## Basic Usage

```python
from poor_man_lakehouse import PyIcebergClient

client = PyIcebergClient()
```

By default, it connects to the Lakekeeper catalog using settings from `.env`. Override with custom values:

```python
client = PyIcebergClient(
    catalog_uri="http://nessie:19120/iceberg",
    catalog_name="nessie",
)
```

## Catalog Browsing

```python
# List namespaces
namespaces = client.list_namespaces()
# [('default',), ('staging',)]

# List tables in a namespace
tables = client.list_tables("default")
# [('default', 'users'), ('default', 'orders')]
```

## Table Operations

### Load a Table

```python
table = client.load_table("default", "users")

# Full PyIceberg Table object — access metadata, schema, partitioning, etc.
print(table.schema())
print(table.metadata.partition_specs)
```

### Schema Inspection

```python
schema = client.table_schema("default", "users")
# [
#   {"field_id": 1, "name": "id", "type": "long", "required": True},
#   {"field_id": 2, "name": "name", "type": "string", "required": False},
#   {"field_id": 3, "name": "age", "type": "int", "required": False},
# ]
```

### Snapshot History

```python
history = client.snapshot_history("default", "users")
# [
#   {
#     "snapshot_id": 123456789,
#     "timestamp_ms": 1711234567890,
#     "summary": {"operation": "append", "added-records": "100"}
#   },
# ]
```

## Scanning Tables

### To Polars LazyFrame

```python
import polars as pl

lf = client.scan_to_polars("default", "users")
result = lf.filter(pl.col("age") > 25).select("name", "age").collect()
```

### To Arrow Table

```python
arrow_table = client.scan_to_arrow("default", "users")
print(arrow_table.schema)
```

## Use Cases

- **Schema evolution auditing**: Inspect schema changes across snapshots
- **Time travel**: Access historical table states via PyIceberg's snapshot API
- **Partition inspection**: Understand how tables are partitioned
- **Lightweight reads**: Scan tables to Polars or Arrow without Spark
- **Metadata operations**: Anything that doesn't require DML (INSERT/UPDATE/DELETE)
