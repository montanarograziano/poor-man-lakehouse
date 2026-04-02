# Catalog-Agnostic Refactor Design

**Date:** 2026-03-31
**Status:** Approved

## Goal

Consolidate the project from 6 user-facing classes down to 3 by unifying the catalog layer via PyIceberg and merging overlapping connectors. Drop Unity Catalog support.

## Architecture: Before and After

### Before (6 user-facing classes)

- `CatalogBrowser` — REST catalog metadata browsing via PyIceberg
- `PyIcebergClient` — schema/snapshot inspection + Polars/Arrow scans via PyIceberg
- `PolarsClient` — Unity/Lakekeeper catalog + SQL via sqlglot + DuckDB export
- `IbisConnection` — multi-engine Ibis wrapper (PySpark/Polars/DuckDB)
- `DremioConnection` — Arrow Flight queries against Dremio
- Spark builders (`get_spark_builder()`) — 5 catalog-specific SparkSession factories

**Problem:** 4 independent `load_catalog()` calls, duplicated browsing/scanning logic, fragmented user API.

### After (3 user-facing entry points)

- `LakehouseConnection` — unified lightweight connector (Polars/DuckDB/Arrow/Ibis)
- `DremioConnection` — unchanged
- Spark builders (`get_spark_builder()`) — 4 catalog-specific factories (Unity removed)

All backed by a single `get_catalog()` factory.

## Supported Catalog Types

| Type | PyIceberg `type` | URI Source | Storage |
|---|---|---|---|
| `lakekeeper` | `rest` | `LAKEKEEPER_SERVER_URI` | MinIO (S3-compatible) |
| `nessie` | `rest` | `NESSIE_REST_URI` | MinIO (S3-compatible) |
| `postgres` | `sql` | `postgresql://host/db` | MinIO (S3-compatible) |
| `glue` | `glue` | N/A (AWS SDK) | Real S3 |

**Dropped:** `unity_catalog` — PyIceberg doesn't support it natively, and it only worked with Polars/Spark.

## New Module: `catalog.py`

A single factory function replacing all scattered `load_catalog()` calls:

```python
LakehouseCatalogType = Literal["nessie", "lakekeeper", "postgres", "glue"]

def get_catalog(
    catalog_type: LakehouseCatalogType | None = None,  # defaults to settings.CATALOG
) -> pyiceberg.Catalog:
```

**Naming:** `LakehouseCatalogType` avoids collision with the `CatalogType` enum in `spark_connector/builder.py`, which also includes Unity Catalog. The Spark enum keeps its name since it's already the public API.

**Catalog config per type:**

- **lakekeeper/nessie:** `type="rest"`, URI from settings, `ICEBERG_STORAGE_OPTIONS` merged
- **postgres:** `type="sql"`, URI built from `POSTGRES_HOST/DB/USER/PASSWORD`
- **glue:** `type="glue"`, region + optional `glue.id`

No class hierarchy — the PyIceberg `Catalog` object is the abstraction. Callers use `get_catalog()` with zero args for the common case.

## New Module: `lakehouse.py`

`LakehouseConnection` replaces `IbisConnection`, `PolarsClient`, `PyIcebergClient`, and `CatalogBrowser`.

### Catalog Browsing

Thin pass-throughs to the PyIceberg `Catalog` / `Table` objects:

```python
conn = LakehouseConnection()
conn.list_namespaces()
conn.list_tables("default")
conn.table_schema("default", "users")
conn.snapshot_history("default", "users")
```

### Native Access

```python
lf = conn.scan_polars("default", "users")        # Polars LazyFrame
arrow_tbl = conn.scan_arrow("default", "users")   # PyArrow Table
duck = conn.duckdb_connection                      # DuckDB with catalog attached
```

### Ibis Access

```python
duck_ibis = conn.ibis_duckdb()
polars_ibis = conn.ibis_polars("default", "users")
spark_ibis = conn.ibis_pyspark()
```

### Design Decisions

- All engines are `@cached_property` — lazy init, no JVM/DuckDB startup until accessed
- DuckDB catalog attachment logic (REST vs Glue SQL) moves from `IbisConnection`
- `sql()` supports DuckDB and PySpark, not Polars
- `write_table()` and `create_table()` via DuckDB stay
- Context manager support stays

### Dropped Features

- `PolarsClient.sql()` with sqlglot parsing — fragile; users use `scan_polars()` + native Polars expressions or DuckDB SQL
- `PolarsClient.to_duckdb()` — redundant with `conn.duckdb_connection` which has the full catalog
- `PolarsClient.show_catalog_tree()` — `list_namespaces()` + `list_tables()` cover it
- `load_sql_magic` (Jupyter `%%sql`) — Unity-specific
- `PolarsClient.load_all_tables()` — users iterate `list_tables()` + `scan_polars()` if needed

## Changes to Existing Modules

### `spark_connector/builder.py`

- Delete `DeltaUnityCatalogSparkBuilder`
- Remove `UNITY_CATALOG` from `CatalogType` enum
- Remove Unity Catalog package from `COMMON_PACKAGES`

### `config.py`

- Remove `UNITY_CATALOG_URI`
- Update `CATALOG` to reflect supported types: `nessie`, `lakekeeper`, `postgres`, `glue`
- Remove `require_catalog()` — PyIceberg handles all types now

### `__init__.py`

```python
__all__ = [
    # Catalog
    "get_catalog",
    # Config
    "Settings", "get_settings", "reload_settings", "settings",
    # Connectors
    "LakehouseConnection",
    "DremioConnection",
    # Spark
    "CatalogType", "get_spark_builder", "retrieve_current_spark_session",
]
```

## Deleted Modules

- `catalog_browser.py`
- `pyiceberg_connector/` (entire package)
- `polars_connector/` (entire package)
- `ibis_connector/` (entire package)

## File Structure After Refactor

```
src/poor_man_lakehouse/
├── __init__.py
├── config.py
├── catalog.py              # NEW
├── lakehouse.py            # NEW
├── spark_connector/
│   ├── __init__.py
│   └── builder.py
└── dremio_connector/
    ├── __init__.py
    └── builder.py
```

## Testing Strategy

- Delete tests for `CatalogBrowser`, `PyIcebergClient`, `PolarsClient`, `IbisConnection`
- New unit tests for `get_catalog()`: mock `load_catalog`, verify config dict per catalog type
- New unit tests for `LakehouseConnection`: mock catalog, test browsing/scan/ibis methods
- Update existing `spark_connector` tests to remove Unity references
