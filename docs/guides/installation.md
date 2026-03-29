# Installation

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| [Docker](https://docs.docker.com/get-docker/) | 20.10+ | Container runtime for all services |
| [uv](https://github.com/astral-sh/uv) | 0.4+ | Python package manager |
| [just](https://github.com/casey/just) | 1.0+ | Command runner |
| Python | 3.12+ | Runtime |
| Java | 17+ | Required only for PySpark |

## Steps

### 1. Clone the Repository

```bash
git clone https://github.com/montanarograziano/poor-man-lakehouse.git
cd poor-man-lakehouse
```

### 2. Install Dependencies

```bash
just install
```

This runs `uv sync --all-groups` and installs pre-commit hooks.

### 3. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` to set your preferred catalog:

```dotenv
CATALOG="lakekeeper"          # Options: nessie, lakekeeper, postgres, unity_catalog
CATALOG_NAME="lakekeeper"
AWS_ACCESS_KEY_ID="minioadmin"
AWS_SECRET_ACCESS_KEY="miniopassword"
```

See [Configuration](configuration.md) for all available settings.

### 4. Set Up Local DNS (Important)

The `.env` file uses Docker service names (`minio`, `postgres_db`, `nessie`, `lakekeeper`). For local Python development outside Docker, add them to your hosts file:

```bash
echo "127.0.0.1 minio postgres_db nessie lakekeeper dremio" | sudo tee -a /etc/hosts
```

This allows the same `.env` to work both inside Docker containers and for local development.

### 5. Start Services

```bash
just up lakekeeper   # Recommended: Core + Lakekeeper catalog
```

### 6. Verify

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / miniopassword |
| Lakekeeper API | http://localhost:8181 | - |
| Nessie API | http://localhost:19120 | - |
| Spark Master UI | http://localhost:8081 | - |
| Dremio UI | http://localhost:9047 | dremio / dremio123 |

## Useful Commands

```bash
just help            # List all available commands
just up nessie       # Start with Nessie catalog
just down            # Stop all services
just up-clean nessie # Clean restart (wipes data)
just lint            # Run ruff + mypy
just test            # Run all tests
just test-coverage   # Tests with coverage report
just logs            # Follow Docker logs
```
