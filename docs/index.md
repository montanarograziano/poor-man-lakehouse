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
|   IbisConnection    PolarsClient    PyIcebergClient            |
|   (multi-engine)    (SQL + scan)   (table management)          |
+---------------------------------------------------------------+
|  PySpark | Polars | DuckDB | Sail (Rust) | Dremio (Flight)    |
+---------------------------------------------------------------+
|                      Catalog Layer                              |
|   Nessie  |  Lakekeeper  |  Unity Catalog  |  PostgreSQL       |
+---------------------------------------------------------------+
|                      Storage Layer                              |
|                   MinIO (S3-compatible)                         |
+---------------------------------------------------------------+
```

## Components at a Glance

| Component | Purpose | Catalog Requirement |
|-----------|---------|-------------------|
| [`IbisConnection`](guides/ibis-connector.md) | Multi-engine access (PySpark, Polars, DuckDB) + DuckDB writes | Lakekeeper |
| [`PyIcebergClient`](guides/pyiceberg-connector.md) | Schema inspection, snapshots, time travel (no JVM) | Any REST catalog |
| [`CatalogBrowser`](guides/catalog-browser.md) | Catalog-agnostic namespace/table browsing | Any REST catalog |
| [`PolarsClient`](guides/polars-connector.md) | SQL queries + catalog browsing via Polars | Unity or Lakekeeper |
| [`SparkBuilder`](guides/spark-connector.md) | Configured SparkSession per catalog type | Any |
| [Sail](guides/connectors-overview.md#sail-pysail) | Rust-powered Spark Connect engine (no JVM) — Delta, Iceberg, S3 | Local file-based |
| [`DremioConnection`](guides/dremio-connector.md) | Arrow Flight query federation | Nessie + Dremio |

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
