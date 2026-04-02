# Configuration

All settings are managed via the `Settings` class in `config.py`, powered by [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/). Values are loaded from environment variables and `.env` file.

## Environment Variables

### Catalog Selection

| Variable | Default | Description |
|----------|---------|-------------|
| `CATALOG` | `nessie` | Active catalog backend: `nessie`, `lakekeeper`, `postgres`, `glue` |
| `CATALOG_NAME` | `nessie` | Catalog name used in Spark/DuckDB/PyIceberg configuration |
| `CATALOG_DEFAULT_SCHEMA` | `default` | Default schema/namespace |

!!! warning "Match CATALOG to Docker Profile"
    Your `CATALOG` setting must match the Docker profile you started:

    | Docker Profile | `CATALOG` Value |
    |---------------|-----------------|
    | `just up nessie` | `nessie` |
    | `just up lakekeeper` | `lakekeeper` |
    | `just up` (core only) | `postgres` |
    | *(no Docker needed)* | `glue` (uses AWS credentials) |

### AWS / MinIO

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | `""` | MinIO access key (or AWS key for Glue) |
| `AWS_SECRET_ACCESS_KEY` | `""` | MinIO secret key (or AWS secret for Glue) |
| `AWS_ENDPOINT_URL` | `http://minio:9000` | S3-compatible endpoint (not used with Glue) |
| `AWS_DEFAULT_REGION` | `eu-central-1` | AWS region |
| `BUCKET_NAME` | `warehouse` | S3 bucket for table data |

### AWS Glue

| Variable | Default | Description |
|----------|---------|-------------|
| `GLUE_CATALOG_ID` | `""` | AWS account ID for cross-account Glue access; empty = default account |

### PostgreSQL

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_USER` | `postgres` | Database user |
| `POSTGRES_PASSWORD` | `""` | Database password |
| `POSTGRES_DB` | `lakehouse_db` | Database name |

### Catalog URIs

| Variable | Default | Used By |
|----------|---------|---------|
| `NESSIE_NATIVE_URI` | `http://nessie:19120/api/v2` | Nessie native API (Spark) |
| `NESSIE_REST_URI` | `http://nessie:19120/iceberg` | Nessie REST/Iceberg API (PyIceberg, DuckDB) |
| `LAKEKEEPER_SERVER_URI` | `http://lakekeeper:8181/catalog` | Lakekeeper REST catalog |

### Spark

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_MASTER` | `spark://localhost:7077` | Spark master URL (use `local[*]` for no cluster) |
| `SPARK_DRIVER_HOST` | `172.18.0.1` | Driver host (Docker bridge IP) |
| `SPARK_DRIVER_PORT` | `7001` | Driver port |
| `SPARK_DRIVER_BLOCK_MANAGER_PORT` | `7002` | Block manager port |

### Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_VERBOSITY` | `DEBUG` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_ROTATION_SIZE` | `100MB` | Log file rotation size |
| `LOG_RETENTION` | `30 days` | Log file retention period |

## Computed Fields

Some settings are computed automatically from other values:

- **`WAREHOUSE_BUCKET`** = `s3://{BUCKET_NAME}/` -- automatically derived from `BUCKET_NAME`
- **`SETTINGS_PATH`** = `{REPO_PATH}/settings` -- derived from `REPO_PATH`
- **`LOG_FILE_PATH`** = `{LOG_FOLDER}/{LOG_FILE_NAME}` -- derived from log settings

These computed fields respect environment variable overrides of their source fields.

## Storage Options

Two computed dictionaries are built at startup based on `CATALOG`:

- **`S3_STORAGE_OPTIONS`** -- S3 credentials, endpoint, region for general S3 access (Polars, Delta)
- **`ICEBERG_STORAGE_OPTIONS`** -- PyIceberg-specific S3 config. For Glue, this contains only region and warehouse. For other catalogs, it includes full S3 credentials and endpoint.

These are consumed internally by `get_catalog()` and `LakehouseConnection`. You rarely need to access them directly.

## Programmatic Access

```python
from poor_man_lakehouse import settings, reload_settings

# Access any setting
print(settings.CATALOG)              # "lakekeeper"
print(settings.AWS_ENDPOINT_URL)     # "http://minio:9000"
print(settings.WAREHOUSE_BUCKET)     # "s3://warehouse/" (computed)
print(settings.GLUE_CATALOG_ID)      # "" (empty = default account)

# Reload settings (clears cache, re-reads .env)
new_settings = reload_settings()
```
