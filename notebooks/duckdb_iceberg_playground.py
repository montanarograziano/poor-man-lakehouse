"""DuckDB + Iceberg playground (marimo): partitioned write, read, merge via Lakekeeper.

Requirements:
- `just up lakekeeper` (uses .env defaults: catalog on :8181, MinIO on :9000)
- `127.0.0.1 minio` in /etc/hosts (vended credentials return the docker-internal endpoint)

Uses `LakehouseConnection`'s DuckDB engine (duckdb-iceberg extension under the hood):
partitioned CREATE TABLE, INSERT, snapshot inspection, MERGE INTO (merge-on-read),
and time travel. Data comes from the benchmark StreamingGenerator so the row count
is tunable. The last cell shows the raw SQL equivalents if you want to go package-free.
"""
# ruff: noqa: S608, N803, N806  # SQL built from trusted constants; marimo cells export CONSTANTS

import marimo

__generated_with = "0.23.3"
app = marimo.App(width="full")


@app.cell
def _():
    """Imports, generator helpers, and tunables."""
    import sys
    import time
    from pathlib import Path

    import marimo as mo
    import pyarrow as pa

    _bench_dir = str((Path(__file__).resolve().parent / "benchmark").resolve())
    if _bench_dir not in sys.path:
        sys.path.insert(0, _bench_dir)

    from helpers import PARTITION_COL, GeneratorSpec, StreamingGenerator
    from helpers.config import ColumnDef, SchemaConfig

    NAMESPACE = "playground"
    TABLE = "events_iceberg"

    row_count = mo.ui.number(value=1_000_000, start=10_000, step=100_000, label="Row count")
    mo.vstack([mo.md("## Parameters"), row_count])
    return (
        ColumnDef,
        GeneratorSpec,
        NAMESPACE,
        PARTITION_COL,
        SchemaConfig,
        StreamingGenerator,
        TABLE,
        mo,
        pa,
        row_count,
        time,
    )


@app.cell
def _(ColumnDef, GeneratorSpec, SchemaConfig, StreamingGenerator, mo, row_count, time):
    """Generate data (trimmed schema: the full benchmark one drags in map/struct noise).

    Canonical generator layout: `id` (int64, unique) + `event_date` (date32, one month
    of days — the partition column) + the columns below.
    """
    schema_cfg = SchemaConfig(
        id_col="id",
        seed=42,
        merge_overlap_ratio=0.1,
        columns=[
            ColumnDef(name="int64_col", type="int64"),
            ColumnDef(name="float64_col", type="float64"),
            ColumnDef(name="category", type="varchar", cardinality=50, avg_length=12),
            ColumnDef(name="is_active", type="boolean"),
        ],
    )
    spec = GeneratorSpec(schema_config=schema_cfg, total_rows=int(row_count.value), seed=42)

    _t0 = time.perf_counter()
    data = StreamingGenerator(spec).arrow_reader().read_all()
    _gen_s = time.perf_counter() - _t0

    mo.md(
        f"Generated **{data.num_rows:,}** rows x {data.num_columns} cols in {_gen_s:.1f}s "
        f"(dates {spec.date_start} → {spec.date_end})"
    )
    return (data,)


@app.cell
def _(NAMESPACE, data, mo):
    """Connect (settings from .env → Lakekeeper) and stage the Arrow data in DuckDB.

    `register` exposes the Arrow table zero-copy as `gen_events`, resolvable from any
    current database via DuckDB replacement scans.
    """
    from poor_man_lakehouse.config import settings
    from poor_man_lakehouse.lakehouse import LakehouseConnection

    conn = LakehouseConnection()
    if NAMESPACE not in conn.list_namespaces():
        conn.create_namespace(NAMESPACE)
    conn.duckdb_connection.con.register("gen_events", data)

    mo.md(f"Connected to **{settings.CATALOG}** — namespaces: `{conn.list_namespaces()}`")
    return conn, settings


@app.cell
def _(NAMESPACE, PARTITION_COL, TABLE, conn, data, mo, pa, time):
    """Partitioned write: CREATE TABLE ... PARTITIONED BY (event_date) + INSERT."""

    def _duck_type(t) -> str:
        if pa.types.is_dictionary(t):
            t = t.value_type
        if pa.types.is_boolean(t):
            return "BOOLEAN"
        if pa.types.is_integer(t):
            return "BIGINT"
        if pa.types.is_floating(t):
            return "DOUBLE"
        if pa.types.is_date(t):
            return "DATE"
        if pa.types.is_timestamp(t):
            return "TIMESTAMP"
        return "VARCHAR"

    schema_sql = ", ".join(f"{f.name} {_duck_type(f.type)}" for f in data.schema)

    conn.drop_table(NAMESPACE, TABLE)
    conn.create_table(NAMESPACE, TABLE, schema_sql, partition_by=PARTITION_COL)

    _t0 = time.perf_counter()
    conn.write_table(NAMESPACE, TABLE, query="SELECT * FROM gen_events")
    _write_s = time.perf_counter() - _t0

    mo.md(
        f"Wrote **{data.num_rows:,}** rows into `{NAMESPACE}.{TABLE}` "
        f"(identity-partitioned by `{PARTITION_COL}`) in **{_write_s:.1f}s**"
    )
    return (schema_sql,)


@app.cell
def _(NAMESPACE, TABLE, conn, mo, settings, time):
    """Read back: full count + partition-pruned read of a single day."""
    FQN = f"{settings.CATALOG_NAME}.{NAMESPACE}.{TABLE}"

    rows_before_merge = conn.scan_duckdb(NAMESPACE, TABLE).count().execute()

    _t0 = time.perf_counter()
    _one_day = conn.sql(f"SELECT count(*) AS n FROM {FQN} WHERE event_date = DATE '2024-01-15'").execute()
    _pruned_s = time.perf_counter() - _t0

    preview = conn.scan_duckdb(NAMESPACE, TABLE).head(5).execute()
    mo.vstack(
        [
            mo.md(
                f"Total rows: **{rows_before_merge:,}** — one-day pruned count: "
                f"**{int(_one_day['n'][0]):,}** in {_pruned_s:.2f}s"
            ),
            preview,
        ]
    )
    return FQN, rows_before_merge


@app.cell
def _(NAMESPACE, TABLE, conn, mo):
    """Snapshots so far (DuckDB-native iceberg_snapshots); remember the pre-merge one."""
    _snaps = conn.inspect_table(NAMESPACE, TABLE, aspect="snapshots").to_pyarrow().to_pylist()
    pre_merge_snapshot = int(max(_snaps, key=lambda s: s["sequence_number"])["snapshot_id"])
    mo.vstack(
        [
            mo.md(f"**{len(_snaps)}** snapshots — pre-merge snapshot id: `{pre_merge_snapshot}`"),
            conn.inspect_table(NAMESPACE, TABLE, aspect="snapshots").execute(),
        ]
    )
    return (pre_merge_snapshot,)


@app.cell
def _(FQN, conn, mo, time):
    """Merge write (merge-on-read): ~10% updates + ~1% brand-new rows.

    The source is materialized in a temp table first — self-referencing MERGE sources
    are better staged than read from the target mid-merge.
    """
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE merge_source AS
        SELECT id, event_date, int64_col, float64_col, 'MERGED' AS category, is_active
        FROM {FQN} WHERE id % 10 = 0
        UNION ALL
        SELECT id + (SELECT max(id) + 1 FROM {FQN}), event_date, int64_col, float64_col, category, is_active
        FROM {FQN} WHERE id % 100 = 3
        """
    )

    _t0 = time.perf_counter()
    conn.execute(
        f"""
        MERGE INTO {FQN} AS t USING merge_source AS s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET category = s.category
        WHEN NOT MATCHED THEN INSERT (id, event_date, int64_col, float64_col, category, is_active)
            VALUES (s.id, s.event_date, s.int64_col, s.float64_col, s.category, s.is_active)
        """
    )
    merge_s = time.perf_counter() - _t0
    mo.md(f"MERGE INTO completed in **{merge_s:.1f}s**")
    return (merge_s,)


@app.cell
def _(FQN, NAMESPACE, TABLE, conn, mo, pre_merge_snapshot, rows_before_merge):
    """Verify the merge and time-travel back to the pre-merge snapshot."""
    rows_after = conn.scan_duckdb(NAMESPACE, TABLE).count().execute()
    _updated = conn.sql(f"SELECT count(*) AS n FROM {FQN} WHERE category = 'MERGED'").execute()
    rows_at_pre_merge = conn.scan_duckdb(NAMESPACE, TABLE, snapshot_id=pre_merge_snapshot).count().execute()

    mo.md(
        f"""
        | | rows |
        |---|---|
        | before merge | {rows_before_merge:,} |
        | after merge | {rows_after:,} (inserted: {rows_after - rows_before_merge:,}) |
        | updated (`category = 'MERGED'`) | {int(_updated["n"][0]):,} |
        | time travel `AT (VERSION => {pre_merge_snapshot})` | {rows_at_pre_merge:,} |
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Going package-free (native DuckDB SQL)

        Everything above is thin sugar over the `iceberg` extension. In any DuckDB session:

        ```sql
        INSTALL iceberg; LOAD iceberg;
        CREATE SECRET minio (TYPE s3, KEY_ID 'minioadmin', SECRET 'miniopassword',
                             REGION 'eu-central-1', ENDPOINT 'localhost:9000',
                             URL_STYLE 'path', USE_SSL false);
        ATTACH 'warehouse' AS lakekeeper (
            TYPE iceberg, ENDPOINT 'http://localhost:8181/catalog', AUTHORIZATION_TYPE 'none');

        CREATE TABLE lakekeeper.playground.t (id BIGINT, event_date DATE)
            PARTITIONED BY (event_date);
        INSERT INTO lakekeeper.playground.t VALUES (1, DATE '2024-01-01');
        UPDATE lakekeeper.playground.t SET id = 2 WHERE id = 1;   -- merge-on-read
        MERGE INTO lakekeeper.playground.t AS t USING (...) AS s ON t.id = s.id ...;
        SELECT * FROM lakekeeper.playground.t AT (VERSION => <snapshot_id>);
        SELECT * FROM iceberg_snapshots(lakekeeper.playground.t);
        ```
        """
    )
    return


if __name__ == "__main__":
    app.run()
