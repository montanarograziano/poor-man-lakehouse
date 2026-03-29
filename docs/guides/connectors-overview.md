# Connectors Overview

Poor Man's Lakehouse provides multiple connectors for different use cases. This guide helps you choose the right one.

## Decision Tree

```
Do you need to write data?
  YES -> IbisConnection (DuckDB engine) or SparkBuilder
  NO  -> Continue...

Do you need a JVM?
  NO  -> PyIcebergClient (metadata) or PolarsClient (queries) or CatalogBrowser (browsing)
  YES -> Continue...

Do you need multi-engine comparison?
  YES -> IbisConnection
  NO  -> SparkBuilder (if you need Spark) or DremioConnection (if you need federation)
```

## Connector Comparison

| Feature | IbisConnection | PyIcebergClient | CatalogBrowser | PolarsClient | SparkBuilder | DremioConnection |
|---------|---------------|-----------------|----------------|-------------|-------------|-----------------|
| **Read tables** | PySpark, Polars, DuckDB | Polars, Arrow | - | Polars | PySpark | Arrow, Polars, DuckDB, Pandas |
| **Write tables** | DuckDB | - | - | - | PySpark | - |
| **Schema inspection** | - | Full schema + history | Schema only | Schema | Via Spark | - |
| **SQL execution** | PySpark, DuckDB | - | - | Polars SQL | Spark SQL | Dremio SQL |
| **Requires JVM** | Only for PySpark engine | No | No | No | Yes | No |
| **Catalog** | Lakekeeper only | Any REST | Any REST | Unity or Lakekeeper | Any | Nessie + Dremio |
| **Context manager** | Yes | No | No | No | No | Yes |

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

### DremioConnection
**Best for:** Query federation across multiple sources, Arrow Flight protocol, when Dremio is your query engine.

```python
with DremioConnection() as conn:
    df = conn.to_polars("SELECT * FROM nessie.default.users")
```
