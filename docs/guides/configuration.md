# Configuration

All settings are managed via the `Settings` class in `config.py`, powered by [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/). Values are loaded from environment variables and `.env` file.

## Environment Variables

### Catalog Selection

| Variable | Default | Description |
|----------|---------|-------------|
| `CATALOG` | `nessie` | Active catalog backend: `nessie`, `lakekeeper`, `postgres`, `unity_catalog` |
| `CATALOG_NAME` | `nessie` | Catalog name used in Spark/Ibis configuration |
| `CATALOG_DEFAULT_SCHEMA` | `default` | Default schema/namespace |

!!! warning "Match CATALOG to Docker Profile"
    Your `CATALOG` setting must match the Docker profile you started:

    | Docker Profile | `CATALOG` Value |
    |---------------|-----------------|
    | `just up nessie` | `nessie` |
    | `just up lakekeeper` | `lakekeeper` |
    | `just up` (core only) | `postgres` |
    | `just up dremio` | `nessie` |

### AWS / MinIO

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | `""` | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | `""` | MinIO secret key |
| `AWS_ENDPOINT_URL` | `http://minio:9000` | S3-compatible endpoint |
| `AWS_DEFAULT_REGION` | `eu-central-1` | AWS region |
| `BUCKET_NAME` | `warehouse` | S3 bucket for table data |

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
| `NESSIE_NATIVE_URI` | `http://nessie:19120/api/v2` | Nessie native API |
| `NESSIE_REST_URI` | `http://nessie:19120/iceberg` | Nessie REST/Iceberg API |
| `LAKEKEEPER_SERVER_URI` | `http://lakekeeper:8181/catalog` | Lakekeeper REST catalog |
| `UNITY_CATALOG_URI` | `http://unity_catalog:8080/` | Unity Catalog |

### Spark

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_MASTER` | `spark://localhost:7077` | Spark master URL |
| `SPARK_DRIVER_HOST` | `172.18.0.1` | Driver host (Docker bridge IP) |
| `SPARK_DRIVER_PORT` | `7001` | Driver port |
| `SPARK_DRIVER_BLOCK_MANAGER_PORT` | `7002` | Block manager port |

### Dremio

| Variable | Default | Description |
|----------|---------|-------------|
| `DREMIO_SERVER_URI` | `http://dremio:9047` | Dremio REST API |
| `ARROW_ENDPOINT` | `grpc://dremio:32010` | Arrow Flight endpoint |
| `DREMIO_USERNAME` | `dremio` | Admin username |
| `DREMIO_ROOT_PASSWORD` | `""` | Admin password |

### Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_VERBOSITY` | `DEBUG` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_ROTATION_SIZE` | `100MB` | Log file rotation size |
| `LOG_RETENTION` | `30 days` | Log file retention period |

## Computed Fields

Some settings are computed automatically from other values:

- **`WAREHOUSE_BUCKET`** = `s3://{BUCKET_NAME}/` — automatically derived from `BUCKET_NAME`
- **`SETTINGS_PATH`** = `{REPO_PATH}/settings` — derived from `REPO_PATH`
- **`LOG_FILE_PATH`** = `{LOG_FOLDER}/{LOG_FILE_NAME}` — derived from log settings

These computed fields respect environment variable overrides of their source fields.

## Programmatic Access

```python
from poor_man_lakehouse import settings, reload_settings

# Access any setting
print(settings.CATALOG)
print(settings.AWS_ENDPOINT_URL)
print(settings.WAREHOUSE_BUCKET)  # Computed from BUCKET_NAME

# Reload settings (clears cache)
new_settings = reload_settings()
```

## Validation Helper

Connectors can validate that the correct catalog is active:

```python
from poor_man_lakehouse.config import require_catalog

# Raises ValueError if CATALOG != "lakekeeper"
require_catalog("lakekeeper", connector_name="IbisConnection")
```
