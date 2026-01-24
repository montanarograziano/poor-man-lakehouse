# Poor Man's Lakehouse

A composable, local Open Source Lakehouse implementation using modern data engineering tools. This project provides a flexible architecture for experimenting with different catalog systems, compute engines, and table formats.

## Features

- **Multiple Catalog Support**: Nessie, Lakekeeper, Unity Catalog, or PostgreSQL-backed Iceberg catalog
- **Multi-Engine Access**: PySpark, Polars, and DuckDB through a unified Ibis interface
- **Table Formats**: Apache Iceberg (primary), Delta Lake support
- **Object Storage**: MinIO (S3-compatible) for local development
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
│     │  Nessie  │ Lakekeeper │ Unity Cat.  │ Postgres │         │
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
# CATALOG="nessie"      # Options: nessie, lakekeeper, postgres, unity_catalog
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

The project uses Docker Compose profiles to manage service groups:

| Profile | Services Started | Use Case |
|---------|-----------------|----------|
| *(none)* | MinIO, PostgreSQL | Core infrastructure only |
| `nessie` | + Nessie | Iceberg catalog with Git-like versioning |
| `lakekeeper` | + Lakekeeper + bootstrap | Alternative Iceberg REST catalog |
| `dremio` | + Nessie + Dremio | Query federation with Arrow Flight |
| `unity` | + Unity Catalog | Databricks Unity Catalog (experimental) |
| `spark` | + Spark Master/Worker | Distributed Spark cluster |
| `full` | All services | Complete stack |

### Combining Profiles

```bash
# Nessie + Spark cluster
docker compose --profile nessie --profile spark up -d

# Stop all services
just down
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CATALOG` | `nessie` | Active catalog: `nessie`, `lakekeeper`, `postgres`, `unity_catalog` |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | `miniopassword` | MinIO secret key |
| `AWS_ENDPOINT_URL` | `http://localhost:9000` | S3 endpoint |
| `POSTGRES_USER` | `user` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `password` | PostgreSQL password |
| `POSTGRES_DB` | `catalog_db` | PostgreSQL database name |

### Matching Catalog to Profile

Ensure your `.env` `CATALOG` setting matches the Docker profile you're using:

| Profile | CATALOG Setting |
|---------|-----------------|
| `nessie` | `CATALOG=nessie` |
| `lakekeeper` | `CATALOG=lakekeeper` |
| `dremio` | `CATALOG=nessie` |
| *(core only)* | `CATALOG=postgres` |

## Usage

### Using the Ibis Connection (Recommended)

```python
from poor_man_lakehouse.ibis.builder import IbisConnection

# Connections are lazily initialized
conn = IbisConnection()

# Use PySpark (starts Spark session on first access)
spark_conn = conn.get_connection("pyspark")
tables = conn.list_tables("pyspark")

# Use Polars (lightweight, no JVM)
polars_conn = conn.get_connection("polars")

# Use DuckDB (in-memory analytics)
duck_conn = conn.get_connection("duckdb")

# Read Iceberg tables
df = conn.read_table("default", "my_table", engine="polars")
```

### Using Spark Builders Directly

```python
from poor_man_lakehouse.spark.builder import get_spark_builder, CatalogType

# Get builder for your configured catalog
builder = get_spark_builder(CatalogType.NESSIE)
spark = builder.get_spark_session()

# Query Iceberg tables
df = spark.sql("SELECT * FROM nessie.default.my_table")
```

### Using Dremio for Query Federation

```python
from poor_man_lakehouse.dremio.builder import DremioConnection

conn = DremioConnection()

# Query via Arrow Flight
result = conn.to_polars("SELECT * FROM nessie.default.my_table")
```

## Project Structure

```
poor-man-lakehouse/
├── src/poor_man_lakehouse/
│   ├── config.py           # Settings management (Pydantic)
│   ├── spark/
│   │   └── builder.py      # Spark session builders for each catalog
│   ├── ibis/
│   │   └── builder.py      # Multi-engine Ibis connection
│   └── dremio/
│       └── builder.py      # Dremio Arrow Flight connection
├── notebooks/              # Example notebooks
│   ├── pyspark_experiments.ipynb
│   ├── ibis_experiments.ipynb
│   ├── pyiceberg_experiments.ipynb
│   └── ...
├── tests/
│   ├── conftest.py         # Shared fixtures
│   └── unit/               # Unit tests
├── configs/                # Service configuration files
├── docker-compose.yml      # Service definitions with profiles
├── Justfile               # Task runner commands
└── pyproject.toml         # Project dependencies and tools
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

| Catalog | Status | Notes |
|---------|--------|-------|
| **Nessie** | Stable | Git-like versioning, REST API, recommended for development |
| **Lakekeeper** | Stable | Simple REST catalog, good for production |
| **PostgreSQL** | Stable | JDBC-based, simplest setup |
| **Unity Catalog** | Experimental | Requires additional configuration |

## Roadmap

- [x] Nessie catalog integration (PyIceberg, Dremio, PySpark)
- [x] Multi-engine support via Ibis (PySpark, Polars, DuckDB)
- [x] Lakekeeper catalog support
- [x] Docker Compose profiles for flexible deployment
- [x] CI/CD with GitHub Actions
- [ ] Unity Catalog full integration
- [ ] DuckLake support
- [ ] Kubernetes deployment manifests
- [ ] MkDocs documentation site

## Troubleshooting

### Services won't start

```bash
# Check service logs
just logs

# Clean restart
just up-clean nessie
```

### Spark can't connect to MinIO

Ensure `AWS_ENDPOINT_URL` uses `minio:9000` (Docker network) not `localhost:9000`:

```bash
# In .env for Docker services
AWS_ENDPOINT_URL="http://minio:9000"

# For local Python (outside Docker)
AWS_ENDPOINT_URL="http://localhost:9000"
```

### Catalog not found

Ensure your `CATALOG` setting in `.env` matches the profile you started:

```bash
# Check current setting
grep CATALOG .env

# Should match: just up <profile>
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `just test`
4. Run linters: `just lint`
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.
