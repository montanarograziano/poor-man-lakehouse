# Spark Builders

The `spark_connector` module provides catalog-specific SparkSession builders. Each builder configures Spark with the correct JARs, extensions, and catalog settings for its backend.

## Factory Pattern

```python
from poor_man_lakehouse import get_spark_builder, CatalogType

# From enum
builder = get_spark_builder(CatalogType.NESSIE)

# From string
builder = get_spark_builder("lakekeeper")

# Get a configured SparkSession
spark = builder.get_spark_session()
```

Or use the current catalog from settings:

```python
from poor_man_lakehouse import retrieve_current_spark_session

# Uses settings.CATALOG to determine which builder
spark = retrieve_current_spark_session()
```

## Available Builders

### NessieCatalogSparkBuilder

Uses Nessie's native catalog with git-like versioning.

```python
builder = get_spark_builder(CatalogType.NESSIE)
spark = builder.get_spark_session()

spark.sql("CREATE TABLE nessie.default.users (id INT, name STRING)")
spark.sql("INSERT INTO nessie.default.users VALUES (1, 'Alice')")
```

### LakekeeperCatalogSparkBuilder

Uses Lakekeeper's REST catalog interface with credential vending (no static S3 keys in Spark config).

```python
builder = get_spark_builder(CatalogType.LAKEKEEPER)
spark = builder.get_spark_session()

spark.sql("SELECT * FROM lakekeeper.default.users").show()
```

!!! note
    Lakekeeper always uses `"lakekeeper"` as the catalog name, regardless of `CATALOG_NAME` setting.

### PostgresCatalogSparkBuilder

Uses PostgreSQL as the Iceberg catalog backend via JDBC. This is the tested and recommended implementation for local development.

```python
builder = get_spark_builder(CatalogType.POSTGRES)
spark = builder.get_spark_session()
```

### GlueCatalogSparkBuilder

Uses AWS Glue as the Iceberg catalog backend. Credentials are resolved via the AWS default credential chain (env vars, `~/.aws/credentials`, IAM role) -- no static S3 keys in Spark config.

```python
builder = get_spark_builder(CatalogType.GLUE)
spark = builder.get_spark_session()
```

Optionally set `GLUE_CATALOG_ID` for cross-account Glue access.

## Supported Catalog Types

| Enum Value | String | Builder Class | Credential Handling |
|------------|--------|---------------|---------------------|
| `CatalogType.POSTGRES` | `"postgres"` | `PostgresCatalogSparkBuilder` | Static S3 keys from settings |
| `CatalogType.NESSIE` | `"nessie"` | `NessieCatalogSparkBuilder` | Static S3 keys from settings |
| `CatalogType.LAKEKEEPER` | `"lakekeeper"` | `LakekeeperCatalogSparkBuilder` | Credential vending (no static keys) |
| `CatalogType.GLUE` | `"glue"` | `GlueCatalogSparkBuilder` | AWS credential chain (no static keys) |

## Common Configuration

All builders share:

- **Iceberg runtime** (4.0 for Scala 2.13, version 1.10.1)
- **Delta Lake** via `configure_spark_with_delta_pip`
- **Hadoop AWS** for S3/MinIO access
- **Nessie Spark extensions** for git-like operations
- **PostgreSQL JDBC driver** for catalog metadata

The JARs are resolved via Maven/Ivy at session creation time.

## Custom App Name

```python
builder = get_spark_builder(CatalogType.NESSIE)
builder._app_name = "My Custom App"
spark = builder.get_spark_session()
```

## Spark Cluster

When using the Docker Spark cluster (`just up spark`), set:

```dotenv
SPARK_MASTER="spark://localhost:7077"
SPARK_DRIVER_HOST="172.18.0.1"  # Docker bridge gateway IP
SPARK_DRIVER_PORT=7001
SPARK_DRIVER_BLOCK_MANAGER_PORT=7002
```

For local mode (no cluster):

```dotenv
SPARK_MASTER="local[*]"
```

See the [Spark Cluster guide](spark-cluster.md) for details on the Docker-based Spark standalone cluster.
