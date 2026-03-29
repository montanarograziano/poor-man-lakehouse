# Ibis Multi-Engine Connection

`IbisConnection` provides lazy access to PySpark, Polars, and DuckDB engines through a common [Ibis](https://ibis-project.org/) interface, all connected to the Lakekeeper REST catalog.

!!! note "Requires Lakekeeper"
    `IbisConnection` only works when `CATALOG=lakekeeper`. It validates this on initialization.

## Basic Usage

```python
from poor_man_lakehouse import IbisConnection

# Connections are lazily initialized
with IbisConnection() as conn:
    # Engine only starts when you first access it
    duck = conn.get_connection("duckdb")
    spark = conn.get_connection("pyspark")
    polars = conn.get_connection("polars")
```

## Reading Tables

Read Iceberg tables through any engine:

```python
with IbisConnection() as conn:
    # Returns an Ibis table expression
    table = conn.read_table("default", "users", "duckdb")

    # Filter and collect
    result = table.filter(table.age > 25).execute()
```

Each engine reads from the same Lakekeeper catalog:

- **PySpark**: Direct Spark catalog access
- **DuckDB**: Via the attached Lakekeeper REST catalog (Iceberg extension)
- **Polars**: Via PyIceberg as intermediary (converted to LazyFrame, registered in Ibis)

## Writing Tables (DuckDB)

DuckDB 1.5+ supports Iceberg writes through REST catalogs:

```python
with IbisConnection() as conn:
    # Create a table
    conn.create_table("default", "users", "id INTEGER, name VARCHAR, age INTEGER")

    # Insert from SQL
    conn.write_table("default", "users", "duckdb",
                     query="SELECT 1, 'Alice', 30")

    # Insert with overwrite
    conn.write_table("default", "users", "duckdb",
                     query="SELECT 2, 'Bob', 25",
                     mode="overwrite")
```

## SQL Execution

Execute SQL through PySpark or DuckDB:

```python
with IbisConnection() as conn:
    # DuckDB SQL
    result = conn.sql("SELECT * FROM lakekeeper.default.users", "duckdb")

    # PySpark SQL
    result = conn.sql("SELECT count(*) FROM default.users", "pyspark")
```

!!! warning "Polars does not support SQL via Ibis"
    Use `read_table()` for Polars access to Iceberg tables.

## Switching Databases

```python
conn.set_current_database("staging", "duckdb")
conn.set_current_database("production", "pyspark")
```

## Engine Types

The `get_connection()` method returns typed backends via `@overload`:

```python
from poor_man_lakehouse.ibis_connector.builder import IbisConnection

conn = IbisConnection()

# Type-narrowed returns
duck: DuckDBBackend = conn.get_connection("duckdb")
spark: PySparkBackend = conn.get_connection("pyspark")
polars: PolarsBackend = conn.get_connection("polars")
```

## Connection Lifecycle

`IbisConnection` supports context managers and explicit cleanup:

```python
# Context manager (recommended)
with IbisConnection() as conn:
    ...  # connections auto-close on exit

# Manual cleanup
conn = IbisConnection()
try:
    ...
finally:
    conn.close()  # clears all cached connections
```
