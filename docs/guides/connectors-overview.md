# Connectors Overview

Poor Man's Lakehouse provides a small set of focused connectors for different use cases. This guide helps you choose the right one.

## Decision Tree

```
Do you need to write data?
  YES -> LakehouseConnection (DuckDB engine), SparkBuilder, or Sail
  NO  -> Continue...

Do you need a JVM?
  NO  -> LakehouseConnection (Polars, DuckDB, Arrow scans) or Sail
  YES -> SparkBuilder or LakehouseConnection.ibis_pyspark()

Do you want PySpark API without a JVM?
  YES -> Sail (Rust-based Spark Connect engine)
```

## Connector Comparison

| Feature | LakehouseConnection | SparkBuilder | Sail |
|---------|-------------------|-------------|------|
| **Read tables** | Polars, Arrow, DuckDB, Ibis (PySpark/Polars/DuckDB) | PySpark | PySpark API |
| **Write tables** | DuckDB (Iceberg) | PySpark | Delta, Iceberg, Parquet |
| **Catalog browsing** | Namespaces, tables, schemas, snapshots | Via Spark SQL | - |
| **SQL execution** | DuckDB, PySpark (via Ibis) | Spark SQL | PySpark SQL |
| **Requires JVM** | Only for `ibis_pyspark()` | Yes | **No (Rust)** |
| **Catalog support** | All (Nessie, Lakekeeper, PostgreSQL, Glue) | All | Local file-based |
| **Context manager** | Yes | No | Manual start/stop |
| **Backed by** | PyIceberg + Ibis | Spark + Iceberg/Delta JARs | Rust Spark Connect |

## When to Use Each

### LakehouseConnection

**Best for:** Most use cases. Catalog browsing, reading tables into Polars/DuckDB/Arrow, writing via DuckDB, multi-engine comparison via Ibis — all without a JVM (except PySpark engine).

```python
from poor_man_lakehouse import LakehouseConnection

with LakehouseConnection() as conn:
    # Browse the catalog (no JVM)
    namespaces = conn.list_namespaces()
    tables = conn.list_tables("default")
    schema = conn.table_schema("default", "users")

    # Scan to Polars (no JVM)
    lf = conn.scan_polars("default", "users")
    result = lf.filter(pl.col("age") > 25).collect()

    # DuckDB SQL with attached Iceberg catalog
    result = conn.sql("SELECT * FROM lakekeeper.default.users WHERE age > 25")

    # Write data via DuckDB
    conn.create_table("default", "output", "id INTEGER, name VARCHAR")
    conn.write_table("default", "output", query="SELECT 1, 'Alice'")

    # Ibis multi-engine access
    duck_ibis = conn.ibis_duckdb()
    spark_ibis = conn.ibis_pyspark()  # this one needs JVM
```

See the [Lakehouse Connector guide](lakehouse-connector.md) for full details.

### SparkBuilder

**Best for:** Full Spark ecosystem access, complex ETL, Spark-specific features (broadcast joins, UDFs, streaming), or when you need Spark SQL specifically.

```python
from poor_man_lakehouse import get_spark_builder, CatalogType

builder = get_spark_builder(CatalogType.LAKEKEEPER)
spark = builder.get_spark_session()

spark.sql("CREATE TABLE lakekeeper.default.users (id INT, name STRING)")
spark.sql("INSERT INTO lakekeeper.default.users VALUES (1, 'Alice')")
df = spark.sql("SELECT * FROM lakekeeper.default.users")
df.show()
```

See the [Spark Builders guide](spark-connector.md) for full details.

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

## Catalog Support Matrix

| Catalog | `get_catalog()` | `LakehouseConnection` | `SparkBuilder` |
|---------|----------------|----------------------|----------------|
| **Nessie** | REST | All features | `NessieCatalogSparkBuilder` |
| **Lakekeeper** | REST | All features | `LakekeeperCatalogSparkBuilder` |
| **PostgreSQL** | SQL | Browsing + scans (no DuckDB attach) | `PostgresCatalogSparkBuilder` |
| **Glue** | Glue | All features | `GlueCatalogSparkBuilder` |
