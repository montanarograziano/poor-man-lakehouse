# Lakehouse Connector

`LakehouseConnection` is the unified lightweight connector that replaces the previous `IbisConnection`, `PolarsClient`, `PyIcebergClient`, and `CatalogBrowser`. It provides catalog browsing, native scans, DuckDB engine access, Ibis multi-engine wrappers, SQL execution, and DuckDB-based writes — all backed by a single PyIceberg catalog via [`get_catalog()`](catalog-factory.md).

## Basic Usage

```python
from poor_man_lakehouse import LakehouseConnection

# Uses settings.CATALOG (nessie, lakekeeper, postgres, or glue)
conn = LakehouseConnection()

# Or specify explicitly
conn = LakehouseConnection(catalog_type="lakekeeper")
```

## Catalog Browsing

Browse namespaces, tables, schemas, and snapshot history — no JVM required:

```python
conn = LakehouseConnection()

# List namespaces
namespaces = conn.list_namespaces()
# ['default', 'staging']

# List tables in a namespace
tables = conn.list_tables("default")
# ['users', 'orders', 'events']

# Get table schema
schema = conn.table_schema("default", "users")
# [
#   {"field_id": 1, "name": "id", "type": "long", "required": True},
#   {"field_id": 2, "name": "name", "type": "string", "required": False},
#   {"field_id": 3, "name": "age", "type": "int", "required": False},
# ]

# Get snapshot history
history = conn.snapshot_history("default", "users")
# [
#   {
#     "snapshot_id": 123456789,
#     "timestamp_ms": 1711234567890,
#     "summary": {"operation": "append", "added-records": "100"}
#   },
# ]

# Load a raw PyIceberg Table object (for advanced metadata operations)
table = conn.load_table("default", "users")
print(table.schema())
print(table.metadata.partition_specs)
```

## Native Scans

Read Iceberg tables directly into Polars or Arrow — no JVM, no DuckDB, just PyIceberg:

### Polars LazyFrame

```python
import polars as pl

lf = conn.scan_polars("default", "users")

# Build queries with Polars expressions
result = (
    lf.filter(pl.col("age") > 25)
    .group_by("department")
    .agg(pl.col("salary").mean())
    .collect()
)
```

### PyArrow Table

```python
arrow_table = conn.scan_arrow("default", "users")
print(arrow_table.schema)
print(arrow_table.num_rows)
```

## DuckDB Engine

Access a DuckDB connection with the Iceberg catalog already attached. For REST catalogs (Lakekeeper, Nessie), the catalog is mounted via the DuckDB Iceberg extension. For Glue, it uses the AWS credential chain.

```python
# The DuckDB connection is lazily initialized on first access
duck = conn.duckdb_connection

# Query tables directly via SQL
result = duck.sql("SELECT * FROM lakekeeper.default.users LIMIT 10")

# Or use as an Ibis DuckDB backend
duck.list_tables()
```

!!! note "DuckDB and PostgreSQL catalog"
    When `CATALOG=postgres`, the DuckDB connection gets S3 credentials configured but no Iceberg catalog is attached (DuckDB doesn't support attaching SQL-backed Iceberg catalogs). Use `scan_polars()` or `scan_arrow()` to read tables instead.

## Ibis Multi-Engine Access

Get typed Ibis backends for DuckDB, Polars, or PySpark:

### DuckDB via Ibis

```python
duck_ibis = conn.ibis_duckdb()
# Same object as conn.duckdb_connection — full DuckDB Ibis backend
result = duck_ibis.sql("SELECT count(*) FROM lakekeeper.default.users")
```

### Polars via Ibis

Polars has no native catalog support, so the table is loaded via PyIceberg and registered in a Polars Ibis connection:

```python
polars_ibis = conn.ibis_polars("default", "users")
# Table is registered as "default.users" in the Polars backend
table = polars_ibis.table("default.users")
result = table.filter(table.age > 25).execute()
```

### PySpark via Ibis

```python
spark_ibis = conn.ibis_pyspark()
# Full PySpark Ibis backend connected to the current Spark session
result = spark_ibis.sql("SELECT * FROM default.users")
```

!!! warning "PySpark requires a JVM"
    `ibis_pyspark()` calls `retrieve_current_spark_session()`, which starts a JVM and Spark session. The other methods (DuckDB, Polars, Arrow scans) are JVM-free.

## SQL Execution

Execute SQL queries through DuckDB or PySpark:

```python
# DuckDB (default engine)
result = conn.sql("SELECT * FROM lakekeeper.default.users WHERE age > 25")

# PySpark
result = conn.sql("SELECT count(*) FROM default.users", engine="pyspark")
```

The `engine` parameter accepts `"duckdb"` (default) or `"pyspark"`. Returns an Ibis table expression.

## Writing Tables (DuckDB)

DuckDB 1.5+ supports Iceberg writes through REST catalogs:

### Create a Table

```python
conn.create_table("default", "users", "id INTEGER, name VARCHAR, age INTEGER")
```

### Insert from SQL

```python
conn.write_table("default", "users", query="SELECT 1, 'Alice', 30")
```

### Insert with Overwrite

```python
conn.write_table("default", "users", query="SELECT 2, 'Bob', 25", mode="overwrite")
```

### Insert from Ibis Expression

```python
duck = conn.ibis_duckdb()
source = duck.sql("SELECT * FROM lakekeeper.default.staging_users")
conn.write_table("default", "users", data=source, mode="append")
```

!!! note "Write modes"
    - `"append"` (default): `INSERT INTO` — adds rows to existing data
    - `"overwrite"`: `INSERT OVERWRITE` — replaces all existing data

## Connection Lifecycle

`LakehouseConnection` supports context managers for automatic cleanup:

```python
# Context manager (recommended)
with LakehouseConnection() as conn:
    namespaces = conn.list_namespaces()
    lf = conn.scan_polars("default", "users")
    # DuckDB connection auto-closes on exit

# Manual cleanup
conn = LakehouseConnection()
try:
    result = conn.sql("SELECT 1")
finally:
    conn.close()  # clears cached DuckDB connection
```

The `close()` method clears the cached `duckdb_connection` property. The PyIceberg catalog itself is lightweight and doesn't need explicit cleanup.

## Full API Summary

| Category | Method | Returns | JVM Required |
|----------|--------|---------|--------------|
| **Browsing** | `list_namespaces()` | `list[str]` | No |
| | `list_tables(namespace)` | `list[str]` | No |
| | `table_schema(namespace, table)` | `list[dict]` | No |
| | `snapshot_history(namespace, table)` | `list[dict]` | No |
| | `load_table(namespace, table)` | PyIceberg `Table` | No |
| **Scans** | `scan_polars(namespace, table)` | `pl.LazyFrame` | No |
| | `scan_arrow(namespace, table)` | `pa.Table` | No |
| **DuckDB** | `duckdb_connection` (property) | Ibis DuckDB backend | No |
| **Ibis** | `ibis_duckdb()` | Ibis DuckDB backend | No |
| | `ibis_polars(namespace, table)` | Ibis Polars backend | No |
| | `ibis_pyspark()` | Ibis PySpark backend | **Yes** |
| **SQL** | `sql(query, engine="duckdb")` | Ibis table expression | Only if `engine="pyspark"` |
| **Write** | `write_table(namespace, table, ...)` | `None` | No |
| | `create_table(namespace, table, schema)` | `None` | No |
| **Lifecycle** | `close()` | `None` | No |
