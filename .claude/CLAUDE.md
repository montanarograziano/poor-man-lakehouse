# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Poor Man's Lakehouse is a composable, local OSS lakehouse with multiple catalog backends (Nessie, Lakekeeper, Unity Catalog, PostgreSQL), compute engines (PySpark, Polars, DuckDB), and table formats (Iceberg, Delta). All storage goes through MinIO (S3-compatible). Python 3.12+.

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
just up dremio          # Core + Nessie + Dremio
just down               # Stop all
```

## Architecture

The package lives under `src/poor_man_lakehouse/` with four connector subpackages:

- **`spark_connector/`** — SparkSession builders for each catalog type. Uses an abstract `SparkBuilder` base class with subclasses per catalog (`PostgresCatalogSparkBuilder`, `NessieCatalogSparkBuilder`, `LakekeeperCatalogSparkBuilder`, `DeltaUnityCatalogSparkBuilder`). Factory function `get_spark_builder(catalog_type)` selects the right one based on `CatalogType` enum or the `CATALOG` env var.
- **`ibis_connector/`** — `IbisConnection` provides lazy multi-engine access (PySpark, Polars, DuckDB) through Ibis. **Requires Lakekeeper catalog.** Each engine is a `@cached_property` that only initializes when accessed. Uses `@overload` on `get_connection()` so the return type narrows to the specific backend (`DuckDBBackend`, `PolarsBackend`, `PySparkBackend`).
- **`polars_connector/`** — `PolarsClient` for Unity Catalog with `%%sql` Jupyter magic. Reads Delta tables via PyIceberg, provides catalog browsing, SQL (via sqlglot parsing), and lazy evaluation.
- **`dremio_connector/`** — `DremioConnection` for Arrow Flight queries against Dremio. Output methods: `to_polars()`, `to_duckdb()`, `to_pandas()`, `to_arrow()`.

**`config.py`** — Pydantic `Settings` class loaded from `.env`. Singleton via `get_settings()`. All connectors import `settings` from here. Key setting: `CATALOG` determines which catalog backend is active.

**Top-level `__init__.py`** re-exports all major classes so both `from poor_man_lakehouse import IbisConnection` and `from poor_man_lakehouse.ibis_connector import IbisConnection` work.

## Code Conventions

- Line length: 120
- Docstrings: Google style
- Type hints: use `from __future__ import annotations` and `TYPE_CHECKING` for heavy imports (e.g., ibis backends, PySpark)
- Linting: ruff (format + check) then mypy
- Commits: conventional commits via `cz commit`
- Test markers: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.slow`

## Key Constraints

- Only one catalog should run at a time — they share PostgreSQL and MinIO
- `IbisConnection` only works with `CATALOG=lakekeeper`
- PySpark imports are deferred (lazy) to avoid JVM startup at import time
- The `spark_connector/` directory was renamed from `spark/` — all imports must use `spark_connector`
