# Polars Client

`PolarsClient` provides a SQL interface for querying catalog tables using Polars, with support for both Unity Catalog and Lakekeeper backends.

## Unity Catalog Backend (Default)

```python
from poor_man_lakehouse import PolarsClient

client = PolarsClient()
# or explicitly:
client = PolarsClient(backend="unity")
```

## Lakekeeper Backend

```python
client = PolarsClient(backend="lakekeeper")
```

When using the Lakekeeper backend, tables are scanned via PyIceberg and `pl.scan_iceberg()`.

## Catalog Browsing

```python
# List catalogs
catalogs = client.list_catalogs()

# List namespaces
namespaces = client.list_namespaces("unity")

# List tables
tables = client.list_tables("unity", "default")

# Get table schema
schema = client.get_table_schema("unity", "default", "users")

# Describe table
desc = client.describe_table("unity", "default", "users")
print(desc)
# shape: (3, 2)
# +--------+--------+
# | column | dtype  |
# +--------+--------+
# | id     | Int64  |
# | name   | Utf8   |
# | age    | Int32  |
# +--------+--------+

# Visual tree
print(client.show_catalog_tree())
```

## SQL Queries

Run SQL with fully qualified table names (`catalog.namespace.table`):

```python
# Eager execution (returns DataFrame)
df = client.sql("SELECT * FROM unity.default.users WHERE age > 25")

# Lazy execution (returns LazyFrame)
lf = client.sql("SELECT * FROM unity.default.users", lazy=True)
result = lf.filter(pl.col("name").str.starts_with("A")).collect()
```

## Scanning Tables

```python
# Get a LazyFrame for a specific table
lf = client.scan_table("unity", "default", "users")

# Build complex queries with Polars expressions
result = (
    lf.filter(pl.col("age") > 25)
    .group_by("department")
    .agg(pl.col("salary").mean())
    .collect()
)
```

## Loading All Tables

```python
# Load all tables as a dict of LazyFrames
tables = client.load_all_tables(catalog="unity", namespace="default")
# {"users": LazyFrame, "orders": LazyFrame, ...}

# Different naming formats
tables = client.load_all_tables(name_format="full")
# {"unity__default__users": LazyFrame, ...}
```

## DuckDB Bridge

Export all tables to a DuckDB connection:

```python
conn = client.to_duckdb(catalog="unity", namespace="default")
result = conn.sql("SELECT * FROM unity__default__users LIMIT 10")
```

## Jupyter Magic

Enable `%%sql` cell magic in Jupyter notebooks:

```python
from poor_man_lakehouse import PolarsClient, load_sql_magic

client = PolarsClient(backend="lakekeeper")
load_sql_magic(client)
```

Then in any cell:

```sql
%%sql
SELECT * FROM lakekeeper.default.users
WHERE created_at > '2024-01-01'
ORDER BY name
```

Options:

```sql
%%sql --limit 50
SELECT * FROM catalog.namespace.table

%%sql --no-display
SELECT COUNT(*) FROM catalog.namespace.table

%%sql --lazy
SELECT * FROM catalog.namespace.table
```

## Cache Management

Table scans are cached for performance. Clear the cache when data changes:

```python
client.clear_cache()
```
