"""Unity Catalog OSS v0.5.0 demo (marimo): Delta tables from Spark AND DuckDB.

Requirements:
- `just up unity` (UC server on http://localhost:8080, H2-backed, file:// storage)
- JDK 17 or 21 (auto-detected below if JAVA_HOME is stale)

Verified version matrix (2026-07):
- Server: unitycatalog/unitycatalog:v0.5.0
- Spark connector: io.unitycatalog:unitycatalog-spark_4.0_2.13:0.5.0
  (per-Spark-version artifacts since 0.5.0; Spark 3.5/Scala 2.12 no longer supported)
- Delta jars: io.delta:delta-spark_4.0_2.13:4.3.1 (connector requires Delta 4.3+)
- DuckDB 1.5.4 with the `unity_catalog` core extension (renamed from `uc_catalog`)

Storage is file:// on purpose: UC OSS still cannot vend credentials for
S3-compatible endpoints (no s3.endpoint property; issues #844/#1324), and
file:// locations skip credential vending in the server, the Spark connector,
and the DuckDB extension. The compose service bind-mounts ./warehouse/unity at
the same absolute path inside the container so both sides resolve identical URIs.
"""
# ruff: noqa: N803, N806, S310  # marimo cells export CONSTANTS; UC endpoint is local http

import marimo

__generated_with = "0.23.3"
app = marimo.App(width="full")


@app.cell
def _():
    """Imports and configuration."""
    import json
    import shutil
    import subprocess
    import urllib.error
    import urllib.request
    from pathlib import Path

    import marimo as mo

    REPO_ROOT = Path(__file__).resolve().parent.parent
    UC_URI = "http://localhost:8080"
    UC_API = f"{UC_URI}/api/2.1/unity-catalog"
    CATALOG = "unity"
    WAREHOUSE = REPO_ROOT / "warehouse" / "unity"

    # unitycatalog-spark 0.5.0 ships per-Spark-version artifacts and needs Delta 4.3+.
    # Explicit Maven coordinates instead of configure_spark_with_delta_pip, which would
    # pull the repo-pinned Delta 4.0.1 jars that the connector rejects.
    SPARK_PACKAGES = [
        "io.unitycatalog:unitycatalog-spark_4.0_2.13:0.5.0",
        "io.delta:delta-spark_4.0_2.13:4.3.1",
    ]

    def uc_request(method: str, path: str, payload: dict | None = None) -> dict | None:
        """Call the UC REST API; returns parsed JSON or None on HTTP errors."""
        req = urllib.request.Request(
            f"{UC_API}{path}",
            method=method,
            data=json.dumps(payload).encode() if payload is not None else None,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                body = resp.read()
                return json.loads(body) if body.strip().startswith(b"{") else None
        except urllib.error.HTTPError:
            return None

    return (
        CATALOG,
        REPO_ROOT,
        SPARK_PACKAGES,
        UC_URI,
        WAREHOUSE,
        mo,
        shutil,
        subprocess,
        uc_request,
    )


@app.cell
def _(CATALOG, WAREHOUSE, mo, uc_request):
    """Bootstrap: catalog + schemas (idempotent).

    The catalog gets a file:// storage_root so MANAGED tables work: the server
    places them under `<storage_root>/__unitystorage/...`, a path the container
    resolves identically thanks to the bind mount.

    Two schemas on purpose:
    - `default`: tables created by the Spark connector. DuckDB cannot list this
      schema today (see the type_precision bug demonstrated below).
    - `interop`: externally-registered tables that BOTH engines can use.
    """
    server_up = uc_request("GET", "/catalogs") is not None
    if not server_up:
        raise RuntimeError("Unity Catalog is not reachable on :8080 — run `just up unity` first")

    uc_request(
        "POST",
        "/catalogs",
        {
            "name": CATALOG,
            "comment": "Poor Man Lakehouse demo catalog",
            "storage_root": WAREHOUSE.as_uri(),
        },
    )
    for _schema in ("default", "interop"):
        uc_request("POST", "/schemas", {"name": _schema, "catalog_name": CATALOG})

    _schemas = [s["name"] for s in (uc_request("GET", f"/schemas?catalog_name={CATALOG}") or {}).get("schemas", [])]
    mo.md(f"UC server OK — catalog **{CATALOG}** with schemas: `{_schemas}`")
    return


@app.cell
def _(CATALOG, REPO_ROOT, SPARK_PACKAGES, UC_URI, mo, subprocess):
    """Spark session with the Unity Catalog connector (UCSingleCatalog)."""
    import os

    # Repair a stale JAVA_HOME (pyspark needs JDK 17/21)
    _java = os.path.join(os.environ.get("JAVA_HOME", ""), "bin", "java")
    if not os.path.exists(_java):
        for _v in ("21", "17"):
            _probe = subprocess.run(["/usr/libexec/java_home", "-v", _v], capture_output=True, text=True, check=False)
            if _probe.returncode == 0:
                os.environ["JAVA_HOME"] = _probe.stdout.strip()
                break

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("unity-catalog-demo")
        .master("local[*]")
        .config("spark.jars.packages", ",".join(SPARK_PACKAGES))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog")
        .config(f"spark.sql.catalog.{CATALOG}", "io.unitycatalog.spark.UCSingleCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.uri", UC_URI)
        .config(f"spark.sql.catalog.{CATALOG}.token", "")
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.warehouse.dir", str(REPO_ROOT / "spark-warehouse"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    mo.md(f"Spark **{spark.version}** connected to UC at `{UC_URI}` (catalog `{CATALOG}`)")
    return (spark,)


@app.cell
def _(mo):
    mo.md(
        r"""
        ## 1. Spark creates a MANAGED Delta table

        With server + connector at 0.5.0 this goes through the new **UC Delta API**
        (`/api/2.1/unity-catalog/delta/v1/...`): the table is *catalog-managed*
        (`delta.feature.catalogManaged`), UC acts as the commit coordinator, and the
        server chooses the location under the catalog's `__unitystorage` root.

        Expect harmless `reportMetrics ... 404` warnings on stderr: the connector's
        async metrics hook calls an endpoint UC OSS does not implement.
        """
    )
    return


@app.cell
def _(CATALOG, spark):
    """Create + populate the managed table from Spark."""
    _t = f"{CATALOG}.default.demo_events"
    spark.sql(f"DROP TABLE IF EXISTS {_t}")
    spark.sql(f"CREATE TABLE {_t} (id BIGINT, name STRING, source STRING) USING DELTA")
    spark.sql(f"INSERT INTO {_t} VALUES (1, 'alpha', 'spark'), (2, 'beta', 'spark')")
    spark.sql(f"SELECT * FROM {_t} ORDER BY id")
    return


@app.cell
def _(CATALOG, spark):
    """Where did UC put it? (server-assigned location under __unitystorage)."""
    _rows = spark.sql(f"DESCRIBE EXTENDED {CATALOG}.default.demo_events").collect()
    [r.data_type for r in _rows if r.col_name == "Location"]
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## 2. An `interop` table both engines can use

        DuckDB's `unity_catalog` extension **cannot create tables** (`CREATE TABLE`
        is `Not implemented`) and — as of extension build `dbca44d` / nightly `e37b1b4`
        vs server v0.5.0 — it cannot even *list* a schema containing Spark-connector
        tables: v0.5.0 serializes column `type_precision`/`type_scale` as `null`,
        and the extension's parser requires integers
        (`ParseColumnDefinition` in `src/uc_api.cpp`).

        Workaround demonstrated here: write the Delta data with **delta-rs** (Polars),
        then register it as an **EXTERNAL** table via the UC REST API with explicit
        `type_precision: 0` — which the server stores and echoes back as integers.
        """
    )
    return


@app.cell
def _(CATALOG, WAREHOUSE, mo, shutil, uc_request):
    """Seed + register the external interop table (reset on each full run)."""
    import polars as pl

    INTEROP_TABLE = f"{CATALOG}.interop.ext_demo"
    _location = WAREHOUSE / "interop" / "ext_demo"

    uc_request("DELETE", f"/tables/{INTEROP_TABLE}")
    shutil.rmtree(_location, ignore_errors=True)

    pl.DataFrame({"id": [10, 11], "name": ["ext-a", "ext-b"], "source": ["deltalake", "deltalake"]}).write_delta(
        str(_location)
    )

    def _col(name: str, type_text: str, type_name: str, type_json_type: str, position: int) -> dict:
        return {
            "name": name,
            "type_text": type_text,
            "type_name": type_name,
            "type_json": f'{{"name":"{name}","type":"{type_json_type}","nullable":true,"metadata":{{}}}}',
            "position": position,
            "nullable": True,
            "type_precision": 0,  # explicit ints keep DuckDB's strict parser happy
            "type_scale": 0,
        }

    _created = uc_request(
        "POST",
        "/tables",
        {
            "name": "ext_demo",
            "catalog_name": CATALOG,
            "schema_name": "interop",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": _location.as_uri(),
            "columns": [
                _col("id", "bigint", "LONG", "long", 0),
                _col("name", "string", "STRING", "string", 1),
                _col("source", "string", "STRING", "string", 2),
            ],
        },
    )
    mo.md(f"Registered `{INTEROP_TABLE}` at `{_created['storage_location']}`")
    return (INTEROP_TABLE,)


@app.cell
def _(mo):
    mo.md(
        r"""
        ## 3. DuckDB attaches the catalog

        The extension is now named `unity_catalog` (a core extension; the old
        `uc_catalog`/`TYPE UC` names still work as deprecated aliases). Two gotchas:

        - **Bind the secret explicitly** (`SECRET uc` in ATTACH). With a *named*
          secret left unbound, the extension silently uses an empty endpoint:
          first query fails with `Could not connect`, later ones return empty lists.
        - `file://` table locations skip credential vending entirely — no S3 secret
          gymnastics needed for this local setup.
        """
    )
    return


@app.cell
def _(CATALOG, UC_URI):
    """DuckDB connection: extensions, UC secret, ATTACH."""
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL unity_catalog; INSTALL delta; LOAD delta; LOAD unity_catalog;")
    con.execute(
        f"CREATE SECRET uc (TYPE unity_catalog, TOKEN 'not-used', ENDPOINT '{UC_URI}', AWS_REGION 'eu-central-1')"
    )
    con.execute(f"ATTACH '{CATALOG}' AS uc (TYPE unity_catalog, SECRET uc)")
    con.sql("SELECT database_name, schema_name FROM duckdb_schemas() WHERE database_name = 'uc'")
    return (con,)


@app.cell
def _(con):
    """DuckDB reads the interop table through Unity Catalog."""
    con.sql("SELECT * FROM uc.interop.ext_demo ORDER BY id")
    return


@app.cell
def _(con):
    """DuckDB appends through UC (INSERT is supported since 2026-05; DDL is not)."""
    con.execute("INSERT INTO uc.interop.ext_demo VALUES (12, 'duck-a', 'duckdb'), (13, 'duck-b', 'duckdb')")
    con.sql("SELECT * FROM uc.interop.ext_demo ORDER BY id")
    return


@app.cell
def _(con, mo):
    """Current DuckDB limitations, demonstrated."""
    try:
        con.execute("CREATE TABLE uc.interop.duckdb_made (id INT)")
        _ddl = "UNEXPECTED: CREATE TABLE succeeded"
    except Exception as _e:  # noqa: BLE001
        _ddl = f"`CREATE TABLE` → `{_e}`"

    try:
        con.sql("SELECT * FROM uc.default.demo_events LIMIT 1").fetchall()
        _read = "UNEXPECTED: reading the Spark-managed schema worked (upstream bug fixed? drop the interop workaround)"
    except Exception as _e:  # noqa: BLE001
        _read = f"Reading `uc.default` (Spark-created tables) → `{_e}`"

    mo.md(
        f"""
        - {_ddl}
        - {_read}

        The second failure is the `type_precision: null` incompatibility between
        UC OSS v0.5.0 and the DuckDB extension described above — it poisons every
        listing of a schema that contains Spark-connector-created tables.
        """
    )
    return


@app.cell
def _(mo):
    mo.md("""## 4. Back to Spark: cross-engine verification""")
    return


@app.cell
def _(INTEROP_TABLE, spark):
    """Spark sees delta-rs + DuckDB rows and appends its own."""
    spark.sql(f"INSERT INTO {INTEROP_TABLE} VALUES (20, 'spark-a', 'spark')")
    spark.sql(f"SELECT source, count(*) AS n FROM {INTEROP_TABLE} GROUP BY source ORDER BY source")
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Findings (2026-07)

        | Capability | Status |
        |---|---|
        | Spark 4.0.1 + UC 0.5.0: managed Delta tables (Delta API, catalog-managed commits) | ✅ |
        | Spark: read/append external Delta tables registered in UC | ✅ |
        | DuckDB: attach UC, browse schemas, read Delta tables (`file://`) | ✅ |
        | DuckDB: `INSERT INTO` through UC | ✅ (append-only) |
        | DuckDB: `CREATE TABLE` / `CREATE SCHEMA` / `UPDATE` / `DELETE` / `MERGE` | ❌ not implemented upstream |
        | DuckDB: schemas containing Spark-connector tables | ❌ `type_precision: null` parse bug |
        | MinIO-backed (s3://) UC tables | ❌ UC OSS cannot vend credentials for custom S3 endpoints |

        So "create a Delta table from both engines" lands as: **Spark (or delta-rs +
        REST) creates, both engines read, both engines append.**
        """
    )
    return


if __name__ == "__main__":
    app.run()
