# DuckDB-Native Iceberg Functions for LakehouseConnection — Design

**Date:** 2026-07-09
**Status:** Approved (approach A, full sweep)

## Goal

Expose the Iceberg capabilities the DuckDB `iceberg` extension gained between
DuckDB 1.2 and 1.5.4 through `LakehouseConnection`'s DuckDB engine: time-travel
reads, native metadata inspection, partitioned/CTAS table creation, table drop,
and a modernized catalog attach. UPDATE/DELETE/MERGE need no wrappers (plain SQL
through the existing `sql()` once the catalog is attached); integration tests
prove they work against Lakekeeper.

## Verified upstream state (July 2026, DuckDB 1.5.4)

- Writes through attached REST catalogs: CREATE TABLE/CTAS + INSERT (1.4.0),
  UPDATE/DELETE merge-on-read (1.4.2), MERGE INTO + ALTER TABLE (1.5.3),
  partitioned create/insert (1.5.1; bucket/truncate + partitioned UPDATE 1.5.3).
  Copy-on-write unsupported; merge-on-read only.
- Time travel on attached tables: `AT (VERSION => id)` / `AT (TIMESTAMP => ts)`.
- Metadata table functions accept attached-catalog names: `iceberg_snapshots`,
  `iceberg_metadata`, `iceberg_column_stats`, `iceberg_partition_stats`,
  `iceberg_load_table_response`.
- Attach: `AUTHORIZATION_TYPE 'none'` is the documented no-auth mode (current
  code sends `TOKEN ''`, an empty Bearer). Credential vending is default
  (`ACCESS_DELEGATION_MODE 'vended_credentials'`) and since ~May 2026 carries
  MinIO endpoint + path-style through (duckdb-iceberg #594, #792 closed).
  Remote signing unsupported (#670): Nessie still needs the manual S3 secret,
  so the static secret stays for all catalogs (harmless with Lakekeeper, whose
  vended scoped secrets take precedence).
- Known limits: no Nessie branch support (#969), no sort-order DDL, TRUNCATE
  undocumented.

## API additions (all on `LakehouseConnection`)

```python
def scan_duckdb(namespace, table_name, *, snapshot_id: int | None = None,
                as_of: datetime | str | None = None) -> ir.Table
    # SELECT * FROM <fqn> [AT (VERSION => ...) | AT (TIMESTAMP => ...)]
    # snapshot_id and as_of are mutually exclusive

IcebergTableAspect = Literal["snapshots", "manifests", "column_stats", "partition_stats"]

def inspect_table(namespace, table_name, aspect: IcebergTableAspect = "snapshots") -> ir.Table
    # SELECT * FROM iceberg_<fn>(<fqn>) — DuckDB-native, complements the
    # PyIceberg-based snapshot_history()

def create_table(namespace, table_name, schema_sql, *,
                 partition_by: str | None = None,
                 table_properties: Mapping[str, str] | None = None) -> None
    # extended: PARTITIONED BY (...) and WITH ('format-version' = '3', ...)

def create_table_as(namespace, table_name, *, query: str | None = None,
                    data: ir.Table | None = None) -> None
    # CTAS; exactly one of query/data (staging temp view for data, like write_table)

def drop_table(namespace, table_name, *, if_exists: bool = True) -> None

def execute(statement: str) -> None
    # DuckDB statement execution (UPDATE/DELETE/MERGE/ALTER ...) via raw_sql.
    # Needed because sql() returns an ibis expression and ibis prepends
    # DESCRIBE for schema inference, which only works for queries — verified
    # that MERGE INTO fails through sql() but works through raw_sql.
```

Attach change in `_init_duckdb_s3`: `TOKEN ''` → `AUTHORIZATION_TYPE 'none'`.
A private `_fqn(namespace, table_name)` helper replaces the repeated
`f"{settings.CATALOG_NAME}.{namespace}.{table_name}"`.

Rejected alternatives: full DML wrapper methods (`merge_into(...)` etc.) — SQL
string reassembly with no added value (YAGNI); attach-modernization-only — leaves
time travel and metadata inspection awkward, which was the point of the request.

## Error handling

- `scan_duckdb`: ValueError when both snapshot_id and as_of are given.
- `inspect_table`: ValueError on unknown aspect (mirrors `sql()`/`write_table`
  validation style).
- `create_table_as`: ValueError unless exactly one of query/data.
- Everything else surfaces DuckDB errors unchanged (explicit, no swallowing).

## Testing

- Unit (`tests/unit/test_lakehouse.py`): existing mock pattern (patch settings +
  get_catalog, inject mock duckdb connection); assert generated SQL contains
  AT clauses, PARTITIONED BY/WITH, iceberg_* function names; validation raises.
- Integration (`tests/integration/test_duckdb_iceberg.py`, testcontainers
  Lakekeeper stack via existing fixtures): one staged roundtrip — create
  partitioned table (+ properties), insert, UPDATE/DELETE/MERGE via `sql()`,
  snapshot listing via `inspect_table`, time-travel read via `scan_duckdb`
  with an earlier snapshot id, column stats, CTAS, drop.

## Out of scope

- ALTER TABLE wrappers (plain SQL covers it; documented in tests if needed).
- Dropping the static MinIO secret for Lakekeeper (vended creds verified to
  take precedence; removing the secret would break Nessie).
- Nessie branch/ref support (upstream limitation).
