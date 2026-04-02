# Poor Man's Lakehouse

A composable, local **Open Source Lakehouse** built with modern data engineering tools. Mix and match catalog backends, compute engines, and table formats to build your ideal data platform — all running locally on Docker.

## Why?

Setting up a modern lakehouse stack typically requires cloud infrastructure, vendor lock-in, and complex orchestration. This project gives you the same architecture on your laptop:

- **Experiment freely** with Iceberg, Delta Lake, and different catalog backends
- **Compare engines** side-by-side: PySpark, Polars, DuckDB through a unified interface
- **Learn lakehouse patterns** without cloud costs or complexity
- **Prototype locally** before deploying to production

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

## Components at a Glance

| Component | Purpose | Catalog Support |
|-----------|---------|-----------------|
| [`get_catalog()`](guides/catalog-factory.md) | PyIceberg catalog factory — single source of truth for catalog config | All (Nessie, Lakekeeper, PostgreSQL, Glue) |
| [`LakehouseConnection`](guides/lakehouse-connector.md) | Unified lightweight connector: catalog browsing, Polars/Arrow scans, DuckDB engine, Ibis wrappers, SQL, writes | All (via `get_catalog()`) |
| [`SparkBuilder`](guides/spark-connector.md) | Configured SparkSession per catalog type | All |
| [Sail](guides/connectors-overview.md#sail-pysail) | Rust-powered Spark Connect engine (no JVM) — Delta, Iceberg, S3 | Local file-based |

## Quick Start

```bash
# Clone, install, configure
git clone https://github.com/montanarograziano/poor-man-lakehouse.git
cd poor-man-lakehouse
just install
cp .env.example .env

# Start with Lakekeeper (recommended)
just up lakekeeper

# Open a notebook and start querying
jupyter lab notebooks/
```

See the [Installation Guide](guides/installation.md) for detailed setup instructions.
