# Poor Man's Lakehouse

A composable, local Open Source Lakehouse implementation using modern data engineering tools. This project provides a flexible architecture for experimenting with different catalog systems, compute engines, and table formats.

## Features

- **Multiple Catalog Support**: Nessie, Lakekeeper, PostgreSQL, or AWS Glue — all via a single `get_catalog()` factory
- **Unified Connector**: `LakehouseConnection` provides catalog browsing, Polars/Arrow scans, DuckDB SQL, Ibis multi-engine access, and Iceberg writes — all in one class
- **Multi-Engine Access**: PySpark, Polars, DuckDB through Ibis or native APIs
- **Table Formats**: Apache Iceberg (primary), Delta Lake support
- **Object Storage**: MinIO (S3-compatible) for local development, real S3 for AWS Glue
- **Lazy Initialization**: Engines only start when accessed, reducing startup overhead

## Architecture

```
+---------------------------------------------------------------+
|                      Your Application                          |
+---------------------------------------------------------------+
|   LakehouseConnection          |   Spark Builders              |
|   (Polars, DuckDB, Arrow,      |   (PySpark + Iceberg/Delta)   |
|    Ibis, catalog browsing)     |                               |
+---------------------------------------------------------------+
|                   get_catalog() — PyIceberg                    |
|   Unified catalog factory for all backends                     |
+---------------------------------------------------------------+
|                      Catalog Layer                              |
|   Nessie  |  Lakekeeper  |  PostgreSQL  |  AWS Glue            |
+---------------------------------------------------------------+
|                      Storage Layer                              |
|          MinIO (S3-compatible)  |  AWS S3 (Glue only)          |
+---------------------------------------------------------------+
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [uv](https://github.com/astral-sh/uv) (Python package manager)
- [just](https://github.com/casey/just) (command runner)
- Python 3.12+
- Java 17+ (for PySpark only)

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/montanarograziano/poor-man-lakehouse.git
cd poor-man-lakehouse
just install
```

### 2. Configure Environment

```bash
cp .env.example .env

# Edit .env to set your preferred catalog
# CATALOG="lakekeeper"    # Options: nessie, lakekeeper, postgres, glue

# Add Docker service names to /etc/hosts for local Python development
echo "127.0.0.1 minio postgres_db nessie lakekeeper" | sudo tee -a /etc/hosts
```

### 3. Start Services

```bash
# Start with a specific catalog (pick ONE)
just up lakekeeper   # Core + Lakekeeper catalog (recommended)
just up nessie       # Core + Nessie catalog
just up              # Core only (PostgreSQL JDBC catalog)
```

### 4. Verify Setup

- **MinIO Console**: http://localhost:9001 (minioadmin/miniopassword)
- **Lakekeeper API**: http://localhost:8181 (if using lakekeeper profile)
- **Nessie API**: http://localhost:19120 (if using nessie profile)
- **Spark Master UI**: http://localhost:8081 (if using spark profile)

## Usage

### Catalog Browsing & Scanning (No JVM)

```python
from poor_man_lakehouse import LakehouseConnection

with LakehouseConnection() as conn:
    # Browse the catalog
    print(conn.list_namespaces())
    print(conn.list_tables("default"))

    # Inspect table metadata
    schema = conn.table_schema("default", "users")
    history = conn.snapshot_history("default", "users")

    # Scan to Polars (lazy)
    import polars as pl
    lf = conn.scan_polars("default", "users")
    result = lf.filter(pl.col("age") > 25).collect()

    # Scan to Arrow
    arrow_table = conn.scan_arrow("default", "users")
```

### DuckDB SQL & Iceberg Writes

```python
with LakehouseConnection() as conn:
    # DuckDB has the Iceberg catalog already attached
    result = conn.sql("SELECT * FROM lakekeeper.default.users WHERE age > 25")

    # Create and write tables
    conn.create_table("default", "output", "id INTEGER, name VARCHAR")
    conn.write_table("default", "output", query="SELECT 1, 'Alice'")
```

### Ibis Multi-Engine Comparison

```python
with LakehouseConnection() as conn:
    # DuckDB (no JVM)
    duck = conn.ibis_duckdb()
    result = duck.sql("SELECT count(*) FROM lakekeeper.default.users")

    # Polars (via PyIceberg, no JVM)
    polars_backend = conn.ibis_polars("default", "users")

    # PySpark (requires JVM)
    spark = conn.ibis_pyspark()
```

### Using the Catalog Factory Directly

```python
from poor_man_lakehouse import get_catalog

catalog = get_catalog()  # uses settings.CATALOG
namespaces = catalog.list_namespaces()
table = catalog.load_table("default.users")
```

### PySpark via Spark Builders

```python
from poor_man_lakehouse import get_spark_builder, CatalogType

builder = get_spark_builder(CatalogType.LAKEKEEPER)
spark = builder.get_spark_session()
df = spark.sql("SELECT * FROM lakekeeper.default.users")
```

### AWS Glue Catalog (Cloud)

AWS Glue requires no Docker services — it connects directly to the AWS Glue service.

```bash
# .env settings for Glue
CATALOG="glue"
CATALOG_NAME="glue"
AWS_DEFAULT_REGION="us-east-1"
BUCKET_NAME="my-data-lake"
# GLUE_CATALOG_ID="123456789012"   # Optional: for cross-account access
```

Credential resolution order: env vars > `~/.aws/credentials` > IAM role > AWS SSO.

## Docker Compose Profiles

> **Important**: Run **one catalog at a time**. Multiple catalogs share the same PostgreSQL database and MinIO warehouse, which can cause metadata conflicts.

| Profile      | Services Started         | Use Case                                 |
| ------------ | ------------------------ | ---------------------------------------- |
| *(none)*     | MinIO, PostgreSQL        | Core infrastructure (PostgreSQL catalog)  |
| `nessie`     | + Nessie                 | Iceberg catalog with Git-like versioning |
| `lakekeeper` | + Lakekeeper + bootstrap | REST Iceberg catalog (recommended)       |
| `spark`      | + Spark Master/Worker    | Distributed Spark cluster                |
| `full`       | All services             | **Testing only**                         |

```bash
just up lakekeeper       # Recommended
just down                # Stop all
just up-clean lakekeeper # Clean restart (wipes data)
```

## Project Structure

```
poor-man-lakehouse/
├── src/poor_man_lakehouse/
│   ├── config.py              # Settings management (Pydantic)
│   ├── catalog.py             # get_catalog() — PyIceberg catalog factory
│   ├── lakehouse.py           # LakehouseConnection — unified connector
│   └── spark_connector/       # Spark session builders per catalog
├── docs/                      # Documentation site
├── notebooks/                 # Example notebooks
├── tests/
│   ├── unit/                  # Unit tests (72 tests)
│   └── integration/           # Integration tests (Docker required)
├── configs/                   # Service configuration files
├── docker-compose.yml         # Service definitions with profiles
└── pyproject.toml             # Project dependencies and tools
```

## Supported Catalogs

| Catalog        | Status | PyIceberg Type | Notes                                       |
| -------------- | ------ | -------------- | ------------------------------------------- |
| **Nessie**     | Stable | REST           | Git-like versioning, recommended for dev    |
| **Lakekeeper** | Stable | REST           | Simple REST catalog, good for production    |
| **PostgreSQL** | Stable | SQL            | JDBC-based, simplest local setup            |
| **AWS Glue**   | Stable | Glue           | Cloud-only, no Docker, requires AWS creds   |

## Development

```bash
just install         # Install deps + pre-commit hooks
just lint            # ruff format + ruff check + mypy
just test            # Run all tests
just test-coverage   # Tests with coverage report
just up-clean nessie # Clean restart (wipes data)
```

## Documentation

Full documentation is available at [montanarograziano.github.io/poor-man-lakehouse](https://montanarograziano.github.io/poor-man-lakehouse/).

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `just test`
4. Run linters: `just lint`
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.
