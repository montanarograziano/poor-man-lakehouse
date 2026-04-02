# Catalog Factory

`get_catalog()` is the single source of truth for creating PyIceberg catalog instances. Every other module in the project composes it — `LakehouseConnection`, notebooks, and your own code all go through this one factory.

## Basic Usage

```python
from poor_man_lakehouse import get_catalog

# Uses settings.CATALOG to determine which backend
catalog = get_catalog()

# Or specify explicitly
catalog = get_catalog(catalog_type="lakekeeper")
```

The returned object is a standard [PyIceberg `Catalog`](https://py.iceberg.apache.org/api/#catalog), so you get the full PyIceberg API for free:

```python
catalog.list_namespaces()
# [('default',), ('staging',)]

catalog.list_tables("default")
# [('default', 'users'), ('default', 'orders')]

table = catalog.load_table("default.users")
print(table.schema())
print(table.metadata.snapshots)
```

## Supported Catalog Types

| Type | PyIceberg `type` | URI Source | Storage | Docker Profile |
|------|------------------|-----------|---------|----------------|
| `lakekeeper` | `rest` | `LAKEKEEPER_SERVER_URI` | MinIO (S3-compatible) | `just up lakekeeper` |
| `nessie` | `rest` | `NESSIE_REST_URI` | MinIO (S3-compatible) | `just up nessie` |
| `postgres` | `sql` | Built from `POSTGRES_*` settings | MinIO (S3-compatible) | `just up` (core only) |
| `glue` | `glue` | N/A (AWS SDK) | Real AWS S3 | No Docker needed |

The type is defined as:

```python
LakehouseCatalogType = Literal["nessie", "lakekeeper", "postgres", "glue"]
```

## How Each Backend Is Configured

### Lakekeeper & Nessie (REST catalogs)

Both use PyIceberg's REST catalog type. The config is built by merging `settings.ICEBERG_STORAGE_OPTIONS` (S3 credentials, endpoint, warehouse path) with the catalog-specific REST URI:

```python
{
    "type": "rest",
    "uri": "http://lakekeeper:8181/catalog",  # or NESSIE_REST_URI for nessie
    "s3.endpoint": "http://minio:9000",
    "s3.access-key-id": "...",
    "s3.secret-access-key": "...",
    "warehouse": "...",
}
```

### PostgreSQL (SQL catalog)

Uses PyIceberg's SQL catalog backed by PostgreSQL via the `psycopg` driver. The connection URI is built from individual `POSTGRES_*` settings:

```python
{
    "type": "sql",
    "uri": "postgresql+psycopg://postgres:password@localhost/lakehouse_db",
    "s3.endpoint": "http://minio:9000",
    "warehouse": "s3://warehouse/",
    # ... other S3 settings
}
```

!!! note "psycopg driver"
    The `sql-postgres` PyIceberg extra is already installed, which brings in the `psycopg` driver. No additional dependencies needed.

### AWS Glue

Uses PyIceberg's native Glue catalog type. Credentials are resolved via the AWS default credential chain (env vars, `~/.aws/credentials`, IAM role):

```python
{
    "type": "glue",
    "s3.region": "us-east-1",
    "warehouse": "s3://my-data-lake/",
    # Optional: "glue.id": "123456789012" for cross-account access
}
```

No static S3 credentials are injected — the AWS SDK handles credential resolution.

## Using with LakehouseConnection

You rarely need to call `get_catalog()` directly. `LakehouseConnection` composes it internally:

```python
from poor_man_lakehouse import LakehouseConnection

# This calls get_catalog() under the hood
conn = LakehouseConnection()

# But if you need the raw PyIceberg catalog:
raw_catalog = conn.catalog
```

## Switching Catalogs at Runtime

```python
# Default: uses settings.CATALOG
lakekeeper_catalog = get_catalog()

# Explicit: ignores settings.CATALOG
nessie_catalog = get_catalog(catalog_type="nessie")
glue_catalog = get_catalog(catalog_type="glue")
```

!!! warning "Match your catalog to your Docker services"
    If you started `just up lakekeeper`, only the Lakekeeper catalog will be available. Calling `get_catalog("nessie")` will fail to connect.
