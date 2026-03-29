# Quick Start

This guide gets you from zero to querying Iceberg tables in under 5 minutes.

## Setup

```bash
git clone https://github.com/montanarograziano/poor-man-lakehouse.git
cd poor-man-lakehouse
just install
cp .env.example .env
```

Edit `.env`:

```dotenv
CATALOG="lakekeeper"
CATALOG_NAME="lakekeeper"
```

Start services:

```bash
just up lakekeeper
```

Add Docker hostnames for local development:

```bash
echo "127.0.0.1 minio postgres_db lakekeeper" | sudo tee -a /etc/hosts
```

## Example 1: Browse Catalogs (No JVM Required)

```python
from poor_man_lakehouse import CatalogBrowser

browser = CatalogBrowser()
print(browser.list_namespaces())    # ['default']
print(browser.list_tables("default"))
```

## Example 2: PyIceberg Table Management

```python
from poor_man_lakehouse import PyIcebergClient

client = PyIcebergClient()

# List what's available
namespaces = client.list_namespaces()
tables = client.list_tables("default")

# Load a table and inspect it
table = client.load_table("default", "my_table")
schema = client.table_schema("default", "my_table")
history = client.snapshot_history("default", "my_table")

# Scan to Polars (lazy)
df = client.scan_to_polars("default", "my_table")
result = df.filter(pl.col("id") > 100).collect()
```

## Example 3: Multi-Engine with Ibis

```python
from poor_man_lakehouse import IbisConnection

with IbisConnection() as conn:
    # DuckDB (lightweight, in-memory)
    duck = conn.get_connection("duckdb")
    result = conn.sql("SELECT * FROM lakekeeper.default.my_table", "duckdb")

    # Read a table with any engine
    table = conn.read_table("default", "my_table", "duckdb")

    # Write data via DuckDB (Iceberg write support)
    conn.write_table("default", "my_table", "duckdb", query="SELECT 1 AS id, 'test' AS name")
```

## Example 4: Polars SQL Magic (Jupyter)

```python
from poor_man_lakehouse import PolarsClient, load_sql_magic

# For Lakekeeper
client = PolarsClient(backend="lakekeeper")
load_sql_magic(client)
```

Then in any cell:

```sql
%%sql
SELECT * FROM lakekeeper.default.my_table
WHERE created_at > '2024-01-01'
```

## Example 5: PySpark

```python
from poor_man_lakehouse import get_spark_builder, CatalogType

builder = get_spark_builder(CatalogType.LAKEKEEPER)
spark = builder.get_spark_session()

df = spark.sql("SELECT * FROM lakekeeper.default.my_table")
df.show()
```

## Example 6: Dremio Query Federation

```python
from poor_man_lakehouse import DremioConnection

with DremioConnection() as conn:
    # Query via Arrow Flight
    df = conn.to_polars("SELECT * FROM nessie.default.my_table")
    duck = conn.to_duckdb("SELECT COUNT(*) FROM nessie.default.my_table")
```

## Next Steps

- [Connectors Overview](connectors-overview.md) — understand which connector to use when
- [Configuration](configuration.md) — all available settings
- [Docker Services](docker-services.md) — infrastructure details
