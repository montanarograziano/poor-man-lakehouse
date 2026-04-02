# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Poor Man's Lakehouse is a composable, local OSS lakehouse with multiple catalog backends (Nessie, Lakekeeper, PostgreSQL, AWS Glue), compute engines (PySpark, Polars, DuckDB), and table formats (Iceberg, Delta). Local storage goes through MinIO (S3-compatible); AWS Glue uses real S3. Python 3.12+.

## Commands

```bash
just install            # Install deps + pre-commit hooks
just lint               # ruff format + ruff check + mypy
just test               # Run all tests
just test-coverage      # Tests with coverage report

# Run a single test file or test
uv run pytest tests/unit/test_spark_builder.py -v
uv run pytest tests/unit/test_config.py::TestSettings::test_default_values -v

# Docker services (run ONE catalog at a time)
just up nessie          # Core (MinIO + Postgres) + Nessie
just up lakekeeper      # Core + Lakekeeper
just down               # Stop all
```

## Architecture

The package lives under `src/poor_man_lakehouse/`:

- **`catalog.py`** — `get_catalog()` factory returns a PyIceberg `Catalog` for any supported backend (Lakekeeper, Nessie, PostgreSQL, Glue). Single source of truth for catalog config — all other modules compose it. Defines `LakehouseCatalogType = Literal["nessie", "lakekeeper", "postgres", "glue"]`.
- **`lakehouse.py`** — `LakehouseConnection` is the unified lightweight connector. Provides catalog browsing (namespaces, tables, schemas, snapshots), native scans (Polars LazyFrame, Arrow), DuckDB engine with attached Iceberg catalog, Ibis multi-engine wrappers (DuckDB/Polars/PySpark), SQL execution, and DuckDB-based writes. All backed by `get_catalog()`.
- **`spark_connector/`** — SparkSession builders for each catalog type. Uses an abstract `SparkBuilder` base class with subclasses per catalog (`PostgresCatalogSparkBuilder`, `NessieCatalogSparkBuilder`, `LakekeeperCatalogSparkBuilder`, `GlueCatalogSparkBuilder`). Factory function `get_spark_builder(catalog_type)` selects the right one based on `CatalogType` enum or the `CATALOG` env var.

**`config.py`** — Pydantic `Settings` class loaded from `.env`. Singleton via `get_settings()`. All connectors import `settings` from here. Key setting: `CATALOG` determines which catalog backend is active.

**Top-level `__init__.py`** re-exports: `get_catalog`, `LakehouseCatalogType`, `LakehouseConnection`, `CatalogType`, `get_spark_builder`, `retrieve_current_spark_session`, config utilities.

## Code Conventions

- Line length: 120
- Docstrings: Google style
- Type hints: use `from __future__ import annotations` and `TYPE_CHECKING` for heavy imports (e.g., ibis backends, PySpark)
- Linting: ruff (format + check) then mypy
- Commits: conventional commits via `cz commit`
- Test markers: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.slow`

## Key Constraints

- Only one catalog should run at a time — they share PostgreSQL and MinIO
- AWS Glue catalog is cloud-only (no Docker services) — requires valid AWS credentials and an S3 bucket
- PySpark imports are deferred (lazy) to avoid JVM startup at import time
- The `spark_connector/` directory was renamed from `spark/` — all imports must use `spark_connector`
