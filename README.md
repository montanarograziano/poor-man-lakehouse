# Poor Man's Lakehouse

A composable, local Open Source Lakehouse implementation using modern data engineering tools. This project provides a flexible architecture for experimenting with different catalog systems, compute engines, and table formats.

## Features

- **Multiple Catalog Support**: Nessie, Lakekeeper, Unity Catalog, PostgreSQL-backed Iceberg catalog, or AWS Glue
- **Multi-Engine Access**: PySpark, Polars, and DuckDB through a unified Ibis interface
- **Table Formats**: Apache Iceberg (primary), Delta Lake support
- **Object Storage**: MinIO (S3-compatible) for local development, real S3 for AWS Glue
- **Query Federation**: Dremio integration for cross-source queries via Arrow Flight
- **Lazy Initialization**: Engines only start when accessed, reducing startup overhead

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Your Application                          │
├─────────────────────────────────────────────────────────────────┤
│                     IbisConnection (Lazy)                        │
│              ┌──────────┬──────────┬──────────┐                 │
│              │ PySpark  │  Polars  │  DuckDB  │                 │
├──────────────┴──────────┴──────────┴──────────┴─────────────────┤
│                      Catalog Layer                               │
│     ┌──────────┬────────────┬─────────────┬──────────┐         │
│     │  Nessie  │ Lakekeeper │ Unity Cat.  │ Postgres │  Glue  ││
├─────┴──────────┴────────────┴─────────────┴──────────┴──────────┤
│                      Storage Layer                               │
│                    MinIO (S3-compatible)                         │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [uv](https://github.com/astral-sh/uv) (Python package manager)
- [just](https://github.com/casey/just) (command runner)
- Python 3.12+
- Java 17+ (for PySpark)

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/montanarograziano/poor-man-lakehouse.git
cd poor-man-lakehouse

# Install dependencies
just install
```

### 2. Configure Environment

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env to set your preferred catalog
# CATALOG="nessie"      # Options: nessie, lakekeeper, postgres, unity_catalog, glue

# Add Docker service names to /etc/hosts for local Python development
# This allows the same .env to work both inside Docker and locally
echo "127.0.0.1 minio postgres_db" | sudo tee -a /etc/hosts
```

### 3. Start Services

```bash
# Start core services only (MinIO + PostgreSQL)
just up

# Or start with a specific catalog
just up nessie       # Core + Nessie catalog
just up lakekeeper   # Core + Lakekeeper catalog
just up dremio       # Core + Nessie + Dremio
just up spark        # Core + Spark cluster
just up full         # All services
```

### 4. Verify Setup

- **MinIO Console**: http://localhost:9001 (minioadmin/miniopassword)
- **Nessie API**: http://localhost:19120 (if using nessie profile)
- **Dremio UI**: http://localhost:9047 (if using dremio profile)
- **Spark Master UI**: http://localhost:8081 (if using spark profile)

### 5. Run a Notebook

```bash
# Start Jupyter or use your preferred notebook environment
jupyter lab notebooks/
```

## Docker Compose Profiles

The project uses Docker Compose profiles to manage service groups.

> **Important**: Run **one catalog at a time**. Multiple catalogs (Nessie, Lakekeeper, Unity Catalog, PostgreSQL JDBC) share the same PostgreSQL database and MinIO warehouse. Running them simultaneously can cause metadata conflicts and confusion about which catalog "owns" which tables. The `full` profile is intended for **testing purposes only**.

| Profile      | Services Started         | Use Case                                                   |
| ------------ | ------------------------ | ---------------------------------------------------------- |
| *(none)*     | MinIO, PostgreSQL        | Core infrastructure only (uses PostgreSQL JDBC catalog)    |
| `nessie`     | + Nessie                 | Iceberg catalog with Git-like versioning                   |
| `lakekeeper` | + Lakekeeper + bootstrap | Alternative Iceberg REST catalog                           |
| `dremio`     | + Nessie + Dremio        | Query federation with Arrow Flight                         |
| `unity`      | + Unity Catalog          | Databricks Unity Catalog (experimental)                    |
| `spark`      | + Spark Master/Worker    | Distributed Spark cluster (combine with a catalog profile) |
| `full`       | All services             | **Testing only** - runs all catalogs simultaneously        |

### Recommended Usage

```bash
# Pick ONE catalog and stick with it for your project:
just up              # PostgreSQL JDBC catalog (simplest)
just up nessie       # Nessie catalog (recommended for development)
just up lakekeeper   # Lakekeeper catalog (production-ready REST catalog)

# Add Spark cluster to your chosen catalog:
docker compose --profile nessie --profile spark up -d

# Stop all services
just down
```

### Why One Catalog at a Time?

Each catalog maintains its own metadata about tables in the warehouse:
- A table created via Nessie won't appear in Lakekeeper or the PostgreSQL catalog
- Multiple catalogs can create conflicting metadata for the same storage paths
- Debugging becomes difficult when multiple systems track the same data

## Configuration

### Environment Variables

| Variable                | Default                 | Description                                                         |
| ----------------------- | ----------------------- | ------------------------------------------------------------------- |
| `CATALOG`               | `nessie`                | Active catalog: `nessie`, `lakekeeper`, `postgres`, `unity_catalog`, `glue` |
| `AWS_ACCESS_KEY_ID`     | `minioadmin`            | MinIO access key                                                    |
| `AWS_SECRET_ACCESS_KEY` | `miniopassword`         | MinIO secret key                                                    |
| `AWS_ENDPOINT_URL`      | `http://localhost:9000` | S3 endpoint                                                         |
| `POSTGRES_USER`         | `user`                  | PostgreSQL username                                                 |
| `POSTGRES_PASSWORD`     | `password`              | PostgreSQL password                                                 |
| `POSTGRES_DB`           | `catalog_db`            | PostgreSQL database name                                            |

### Matching Catalog to Profile

Ensure your `.env` `CATALOG` setting matches the Docker profile you're using:

| Profile       | CATALOG Setting      |
| ------------- | -------------------- |
| `nessie`      | `CATALOG=nessie`     |
| `lakekeeper`  | `CATALOG=lakekeeper` |
| `dremio`      | `CATALOG=nessie`     |
| *(core only)* | `CATALOG=postgres`   |

### AWS Glue Catalog (Cloud)

AWS Glue requires no Docker services — it connects directly to the AWS Glue service.

```bash
# .env settings for Glue
CATALOG="glue"
CATALOG_NAME="glue"
AWS_DEFAULT_REGION="us-east-1"     # Must match your Glue catalog's region
BUCKET_NAME="my-data-lake"         # Your S3 bucket for Iceberg data
# GLUE_CATALOG_ID="123456789012"   # Optional: for cross-account access
```

**Credential resolution** (in order):
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM instance profile / role (EC2, ECS, Lambda)
4. AWS SSO / STS assume-role

Both Spark and DuckDB (via Ibis) can read and write Iceberg tables through the Glue catalog.

## Usage

All connectors are available as top-level imports:

```python
from poor_man_lakehouse import (
    IbisConnection,       # Multi-engine (PySpark, Polars, DuckDB) + writes
    PyIcebergClient,      # Schema, snapshots, time travel (no JVM)
    CatalogBrowser,       # Catalog-agnostic browsing
    PolarsClient,         # SQL queries via Polars
    DremioConnection,     # Arrow Flight query federation
    CatalogType,          # Spark catalog enum
    get_spark_builder,    # Spark session factory
    settings,             # Global settings
)
```

### Browse Catalogs (No JVM Required)

```python
from poor_man_lakehouse import CatalogBrowser

browser = CatalogBrowser()  # Auto-resolves URI from CATALOG setting
print(browser.list_namespaces())
print(browser.list_tables("default"))
print(browser.get_table_schema("default", "users"))
```

### PyIceberg Table Management

```python
from poor_man_lakehouse import PyIcebergClient

client = PyIcebergClient()
schema = client.table_schema("default", "users")
history = client.snapshot_history("default", "users")
df = client.scan_to_polars("default", "users")
```

### Multi-Engine with Ibis (Requires Lakekeeper or Glue)

```python
from poor_man_lakehouse import IbisConnection

with IbisConnection() as conn:
    # Read via DuckDB, PySpark, or Polars
    table = conn.read_table("default", "users", "duckdb")
    result = conn.sql("SELECT * FROM lakekeeper.default.users", "duckdb")

    # Write via DuckDB (Iceberg write support in DuckDB 1.5+)
    conn.write_table("default", "users", "duckdb",
                     query="SELECT 1 AS id, 'Alice' AS name")
```

### Polars Client (Unity Catalog or Lakekeeper)

```python
from poor_man_lakehouse import PolarsClient

# Unity Catalog backend (default)
client = PolarsClient()

# Or Lakekeeper backend
client = PolarsClient(backend="lakekeeper")

# Explore and query
print(client.list_tables("unity", "default"))
df = client.sql("SELECT * FROM unity.default.test_table WHERE id > 10")
```

### Using Spark Builders Directly

```python
from poor_man_lakehouse import get_spark_builder, CatalogType

# Get builder for your configured catalog
builder = get_spark_builder(CatalogType.NESSIE)
spark = builder.get_spark_session()

# Query Iceberg tables
df = spark.sql("SELECT * FROM nessie.default.my_table")
```

### Using Dremio for Query Federation

```python
from poor_man_lakehouse import DremioConnection

conn = DremioConnection()

# Query via Arrow Flight — returns Polars DataFrame
result = conn.to_polars("SELECT * FROM nessie.default.my_table")

# Or as DuckDB relation / Pandas DataFrame
duck_rel = conn.to_duckdb("SELECT * FROM nessie.default.my_table")
pandas_df = conn.to_pandas("SELECT * FROM nessie.default.my_table")
```

## Project Structure

```
poor-man-lakehouse/
├── src/poor_man_lakehouse/
│   ├── config.py              # Settings management (Pydantic)
│   ├── catalog_browser.py     # Catalog-agnostic metadata browsing
│   ├── spark_connector/       # Spark session builders per catalog
│   ├── ibis_connector/        # Multi-engine Ibis connection + DuckDB writes
│   ├── pyiceberg_connector/   # Standalone PyIceberg client
│   ├── polars_connector/      # Polars client (Unity + Lakekeeper) + %%sql magic
│   └── dremio_connector/      # Dremio Arrow Flight connection
├── docs/                      # MkDocs documentation site
├── notebooks/                 # Example notebooks
├── tests/
│   ├── unit/                  # Unit tests (76 tests)
│   └── integration/           # Integration tests (Docker required)
├── configs/                   # Service configuration files
├── docker-compose.yml         # Service definitions with profiles
├── mkdocs.yml                 # Documentation site config
├── Justfile                   # Task runner commands
└── pyproject.toml             # Project dependencies and tools
```

## Development

### Running Tests

```bash
# Run all tests
just test

# Run with coverage
uv run pytest tests/ --cov=src/poor_man_lakehouse --cov-report=html
```

### Linting and Type Checking

```bash
# Run all linters (ruff + mypy)
just lint
```

### Clean Restart

```bash
# Remove all data and restart fresh
just up-clean nessie
```

## Supported Catalogs

| Catalog           | Status       | Notes                                                      |
| ----------------- | ------------ | ---------------------------------------------------------- |
| **Nessie**        | Stable       | Git-like versioning, REST API, recommended for development |
| **Lakekeeper**    | Stable       | Simple REST catalog, good for production                   |
| **PostgreSQL**    | Stable       | JDBC-based, simplest setup                                 |
| **Unity Catalog** | Experimental | Requires additional configuration                          |
| **AWS Glue**      | Stable       | Cloud-only, no Docker services, requires AWS credentials   |

## Roadmap

- [x] Nessie catalog integration (PyIceberg, Dremio, PySpark)
- [x] Multi-engine support via Ibis (PySpark, Polars, DuckDB)
- [x] Lakekeeper catalog support
- [x] Docker Compose profiles for flexible deployment
- [x] DuckDB Iceberg write support (DuckDB 1.5+)
- [x] Standalone PyIceberg connector
- [x] Catalog-agnostic browser abstraction
- [x] Polars Lakekeeper backend via PyIceberg
- [x] MkDocs documentation site
- [x] CI/CD with GitHub Actions
- [ ] Unity Catalog full integration
- [ ] DuckLake support
- [ ] Kubernetes deployment manifests

## Troubleshooting

### Services won't start

```bash
# Check service logs
just logs

# Clean restart
just up-clean nessie
```

### Spark/Python can't connect to MinIO

The `.env` uses Docker service names (`minio`, `postgres_db`). For local Python development, add them to your hosts file:

```bash
# Add Docker service names to /etc/hosts
echo "127.0.0.1 minio postgres_db" | sudo tee -a /etc/hosts
```

This allows the same `.env` configuration to work both inside Docker containers and for local development.

### Catalog not found

Ensure your `CATALOG` setting in `.env` matches the profile you started:

```bash
# Check current setting
grep CATALOG .env

# Should match: just up <profile>
```

## Documentation

Full documentation is available at [montanarograziano.github.io/poor-man-lakehouse](https://montanarograziano.github.io/poor-man-lakehouse/).

To preview locally:

```bash
just preview-docs    # Opens at http://localhost:8000
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `just test`
4. Run linters: `just lint`
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.
