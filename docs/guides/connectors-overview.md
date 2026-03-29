# Connectors Overview

Poor Man's Lakehouse provides multiple connectors for different use cases. This guide helps you choose the right one.

## Decision Tree

```
Do you need to write data?
  YES -> IbisConnection (DuckDB engine), SparkBuilder, or Sail
  NO  -> Continue...

Do you need a JVM?
  NO  -> PyIcebergClient, PolarsClient, CatalogBrowser, or Sail
  YES -> Continue...

Do you need multi-engine comparison?
  YES -> IbisConnection
  NO  -> SparkBuilder (if you need Spark) or DremioConnection (if you need federation)

Do you want PySpark API without a JVM?
  YES -> Sail (Rust-based Spark Connect engine)
```

## Connector Comparison

| Feature | IbisConnection | PyIcebergClient | CatalogBrowser | PolarsClient | SparkBuilder | Sail | DremioConnection |
|---------|---------------|-----------------|----------------|-------------|-------------|------|-----------------|
| **Read tables** | PySpark, Polars, DuckDB | Polars, Arrow | - | Polars | PySpark | PySpark API | Arrow, Polars, DuckDB, Pandas |
| **Write tables** | DuckDB | - | - | - | PySpark | Delta, Iceberg, Parquet | - |
| **Schema inspection** | - | Full schema + history | Schema only | Schema | Via Spark | Via PySpark API | - |
| **SQL execution** | PySpark, DuckDB | - | - | Polars SQL | Spark SQL | PySpark SQL | Dremio SQL |
| **Requires JVM** | Only for PySpark engine | No | No | No | Yes | **No (Rust)** | No |
| **Catalog** | Lakekeeper only | Any REST | Any REST | Unity or Lakekeeper | Any | Local file-based | Nessie + Dremio |
| **Context manager** | Yes | No | No | No | No | Manual start/stop | Yes |

## When to Use Each

### IbisConnection
**Best for:** Comparing engines side-by-side, DuckDB Iceberg writes, unified API across PySpark/Polars/DuckDB.

```python
with IbisConnection() as conn:
    # Same table, three engines
    spark_result = conn.read_table("default", "users", "pyspark")
    duck_result = conn.read_table("default", "users", "duckdb")
    polars_result = conn.read_table("default", "users", "polars")
```

### PyIcebergClient
**Best for:** Table metadata operations, snapshot history, schema evolution, time travel — all without a JVM.

```python
client = PyIcebergClient()
schema = client.table_schema("default", "users")
history = client.snapshot_history("default", "users")
df = client.scan_to_polars("default", "users")
```

### CatalogBrowser
**Best for:** Quick catalog exploration across any REST catalog (Nessie or Lakekeeper).

```python
browser = CatalogBrowser()  # Auto-resolves URI from CATALOG setting
namespaces = browser.list_namespaces()
tables = browser.list_tables("default")
schema = browser.get_table_schema("default", "users")
```

### PolarsClient
**Best for:** SQL queries against Unity Catalog or Lakekeeper with Polars, Jupyter `%%sql` magic.

```python
# Unity Catalog
client = PolarsClient()

# Or Lakekeeper
client = PolarsClient(backend="lakekeeper")

df = client.sql("SELECT * FROM catalog.namespace.table WHERE id > 10")
```

### SparkBuilder
**Best for:** Full Spark ecosystem access, complex ETL, when you need Spark-specific features.

```python
builder = get_spark_builder(CatalogType.NESSIE)
spark = builder.get_spark_session()
spark.sql("CREATE TABLE nessie.default.users (id INT, name STRING)")
```

### Sail (pysail)
**Best for:** PySpark-compatible workloads without a JVM, Delta/Iceberg/Parquet reads and writes, fast local development.

Sail is a Rust-based compute engine that implements the Spark Connect protocol. It provides the PySpark API without requiring Java.

```python
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

server = SparkConnectServer()
server.start()
addr = server.listening_address

spark = SparkSession.builder.remote(f"sc://{addr[0]}:{addr[1]}").getOrCreate()

df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.write.format("delta").save("s3://warehouse/my_table")

spark.stop()
server.stop()
```

!!! note "S3 credentials via environment variables"
    Sail reads S3 config from env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`), not from Spark config properties.

!!! warning "No REST catalog support yet"
    Sail does not support Lakekeeper/Nessie REST catalogs. It works with local file-based table formats and direct S3 paths.

### DremioConnection
**Best for:** Query federation across multiple sources, Arrow Flight protocol, when Dremio is your query engine.

```python
with DremioConnection() as conn:
    df = conn.to_polars("SELECT * FROM nessie.default.users")
```
