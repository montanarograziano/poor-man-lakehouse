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

## Example 1: Browse & Scan (No JVM Required)

```python
from poor_man_lakehouse import LakehouseConnection

with LakehouseConnection() as conn:
    # Browse the catalog
    print(conn.list_namespaces())       # ['default']
    print(conn.list_tables("default"))  # ['users', 'orders']

    # Inspect table metadata
    schema = conn.table_schema("default", "users")
    history = conn.snapshot_history("default", "users")

    # Scan to Polars (lazy)
    import polars as pl
    lf = conn.scan_polars("default", "users")
    result = lf.filter(pl.col("id") > 100).collect()

    # Or scan to Arrow
    arrow_table = conn.scan_arrow("default", "users")
```

## Example 2: DuckDB SQL & Writes

```python
from poor_man_lakehouse import LakehouseConnection

with LakehouseConnection() as conn:
    # DuckDB has the Iceberg catalog already attached
    result = conn.sql("SELECT * FROM lakekeeper.default.users WHERE age > 25")

    # Create and write tables via DuckDB
    conn.create_table("default", "output", "id INTEGER, name VARCHAR, age INTEGER")
    conn.write_table("default", "output", query="SELECT 1, 'Alice', 30")
    conn.write_table("default", "output", query="SELECT 2, 'Bob', 25", mode="append")
```

## Example 3: Ibis Multi-Engine Comparison

```python
from poor_man_lakehouse import LakehouseConnection

with LakehouseConnection() as conn:
    # DuckDB (lightweight, no JVM)
    duck = conn.ibis_duckdb()
    result = duck.sql("SELECT count(*) FROM lakekeeper.default.users")

    # Polars (via PyIceberg scan)
    polars_backend = conn.ibis_polars("default", "users")
    table = polars_backend.table("default.users")

    # PySpark (requires JVM)
    spark = conn.ibis_pyspark()
    result = spark.sql("SELECT * FROM default.users")
```

## Example 4: Using the Catalog Factory Directly

```python
from poor_man_lakehouse import get_catalog

# Get a raw PyIceberg catalog
catalog = get_catalog()  # uses settings.CATALOG

# Full PyIceberg API
namespaces = catalog.list_namespaces()
table = catalog.load_table("default.users")
print(table.schema())
print(table.metadata.snapshots)
```

## Example 5: PySpark

```python
from poor_man_lakehouse import get_spark_builder, CatalogType

builder = get_spark_builder(CatalogType.LAKEKEEPER)
spark = builder.get_spark_session()

df = spark.sql("SELECT * FROM lakekeeper.default.my_table")
df.show()
```

## Example 6: Sail -- PySpark Without a JVM

```python
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

# Start Rust-based Spark Connect engine
server = SparkConnectServer()
server.start()
addr = server.listening_address

spark = SparkSession.builder.remote(f"sc://{addr[0]}:{addr[1]}").getOrCreate()

# Same PySpark API, no JVM
df = spark.createDataFrame([(1, "Alice", 30), (2, "Bob", 25)], ["id", "name", "age"])
df.createOrReplaceTempView("users")
spark.sql("SELECT * FROM users WHERE age > 25").show()

# Delta on S3 (requires AWS env vars)
df.write.format("delta").mode("overwrite").save("s3://warehouse/sail_demo/users")

spark.stop()
server.stop()
```

!!! note
    Sail reads S3 credentials from env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`). Set these before starting the kernel.

## Next Steps

- [Connectors Overview](connectors-overview.md) -- understand which connector to use when
- [Catalog Factory](catalog-factory.md) -- how `get_catalog()` works under the hood
- [Lakehouse Connector](lakehouse-connector.md) -- full API reference for `LakehouseConnection`
- [Configuration](configuration.md) -- all available settings
- [Docker Services](docker-services.md) -- infrastructure details
