# Unity Catalog OSS v0.5.0 Integration — Design

**Date:** 2026-07-08
**Status:** Approved (autonomous session; user request was the spec)

## Goal

Re-add Unity Catalog OSS to the compose stack on current versions, verify engine
version constraints, and demonstrate in a notebook that both Spark and DuckDB can
work with a Delta table registered in a local Unity Catalog server.

## Verified version constraints (July 2026)

| Component | Version | Notes |
|---|---|---|
| Unity Catalog OSS server | **v0.5.0** (2026-06-18) | Docker `unitycatalog/unitycatalog:v0.5.0` (multi-arch). `:main` tag is stale (2026-03), do not use. Config path inside image is `/home/unitycatalog/etc/conf` (the old compose mounted `/opt/...`, which the server ignores — likely why the 2026-01 attempt failed). Port 8080. `server.managed-table.enabled` defaults to true. New UC Delta API at `/api/2.1/unity-catalog/delta/v1/...` for catalog-managed commits. |
| UC Spark connector | `io.unitycatalog:unitycatalog-spark_4.0_2.13:0.5.0` | Split per Spark version at 0.5.0. Supports Spark 4.0.x / Scala 2.13 (also `_4.1_` for Spark 4.1). **Spark 3.5 / Scala 2.12 no longer supported.** Requires **Delta 4.3.x** jars (`io.delta:delta-spark_4.0_2.13:4.3.1`). Repo's pyspark 4.0.1 is compatible; repo's `delta-spark==4.0.1` python pin stays — the notebook passes explicit Maven jars instead of `configure_spark_with_delta_pip`. |
| DuckDB | 1.5.4 (already locked) | Extension renamed **`uc_catalog` → `unity_catalog`**, now a core extension (not autoloadable). `CREATE SECRET (TYPE unity_catalog, TOKEN, ENDPOINT, AWS_REGION)`; `ATTACH '<catalog>' (TYPE unity_catalog)`. |
| DuckDB writes to UC | INSERT only | Since 2026-05: `INSERT INTO` works (incl. catalog-managed commits via UC Delta API). `CREATE TABLE/SCHEMA`, `UPDATE`, `DELETE`, `MERGE` **not supported**. So the demo is: Spark creates the table, DuckDB appends and reads. |

## The MinIO constraint (unchanged upstream)

UC OSS still cannot vend credentials for S3-compatible endpoints:

- Server: no `s3.endpoint.N` property exists (verified in `ServerProperties.java` at
  v0.5.0; issues unitycatalog#844 and #1324 still open). Vending is AWS-STS-based;
  the static-credentials path requires a session token that MinIO would reject.
- DuckDB: the extension injects a temporary S3 secret from the vending response
  with no `ENDPOINT`/`URL_STYLE` fields — vended s3:// access always targets AWS.

**Decision: use `file://` storage for the UC demo.** `file://` locations skip
credential vending entirely on both the server and the DuckDB extension, and the
UC Spark connector handles them natively. The compose service bind-mounts
`./warehouse/unity` at the *identical absolute path* inside the container
(`${PWD}/warehouse/unity`) so that host engines (Spark, DuckDB) and the UC server
resolve the same `file://` URIs. This deviates from the repo's MinIO-first
philosophy out of upstream necessity; the notebook documents the limitation and
the compose config keeps the MinIO S3 keys in `server.properties` for future
experiments (e.g. `AWS_ENDPOINT_URL_STS` redirection to MinIO STS).

## Approach chosen

Considered:

1. **Notebook-only Spark config + compose service (chosen).** No `src/` changes.
   The notebook builds its own SparkSession with the UC connector + Delta 4.3.1
   jars. KISS: UC is Delta-first and does not fit the repo's Iceberg-centric
   `SparkBuilder`/`get_catalog()` abstractions without larger design work.
2. Full `CatalogType.UNITY` integration in `spark_connector`/`catalog.py` —
   deferred; UC's Iceberg REST endpoint only serves Iceberg/UniForm tables, and
   the builder currently hardwires Iceberg extensions. Follow-up if the demo
   proves useful.
3. Pin an isolated pyspark 3.5 sandbox for the old `_2.12` connector — obsolete;
   upstream now targets Spark 4 only.

## Components

- **`docker-compose.yml`**: new `unity_catalog` service under profiles
  `["unity", "full"]`, image `v0.5.0`, port 8080, config mounted at
  `/home/unitycatalog/etc/conf`, H2 metadata db persisted under
  `./configs/unity_catalog/etc/data`, warehouse bind-mounted at identical path.
  H2 instead of the shared Postgres: the image's classpath bundling for the
  Postgres JDBC driver is not guaranteed, and UC is self-contained this way
  (Postgres option left as comments in `hibernate.properties`).
- **`configs/unity_catalog/etc/conf/`**: refreshed `server.properties` (v0.5.0
  format) and `hibernate.properties` (H2 file db under `etc/data`).
- **`notebooks/unity_catalog_demo.py`** (marimo, matching repo's newer notebooks):
  1. Spark session with `unitycatalog-spark_4.0_2.13:0.5.0` + `delta-spark_4.0_2.13:4.3.1`,
     `UCSingleCatalog` at `http://localhost:8080`, catalog `unity`.
  2. Spark: `CREATE SCHEMA` / `CREATE TABLE` (Delta) / `INSERT` / `SELECT`.
  3. DuckDB: `ATTACH 'unity' (TYPE unity_catalog)`, browse, `SELECT`,
     `INSERT INTO` (append through UC), demonstrate `CREATE TABLE` is rejected.
  4. Spark re-reads to confirm cross-engine visibility.
- **`Justfile`/docs**: mention `just up unity`.

## Empirical findings from the end-to-end test (2026-07-08)

All verified live against server v0.5.0, connector 0.5.0, DuckDB 1.5.4:

1. **Spark managed tables work fully** (create/insert/select) through the new UC
   Delta API with catalog-managed commits. The connector's async metrics hook logs
   harmless `reportMetrics 404` warnings (endpoint not implemented by UC OSS).
2. **DuckDB ATTACH gotcha:** a *named* UC secret must be bound explicitly
   (`ATTACH ... (TYPE unity_catalog, SECRET <name>)`). Left unbound, the extension
   uses an empty endpoint: the first query fails with `Could not connect` and
   subsequent ones silently return empty schema/table lists.
3. **New upstream incompatibility found:** UC v0.5.0 serializes column
   `type_precision`/`type_scale` as `null`; the DuckDB extension's
   `ParseColumnDefinition` (`src/uc_api.cpp`, builds `dbca44d` and nightly
   `e37b1b4`) requires integers and fails with
   `Invalid field found while parsing field: type_precision`. Any schema holding a
   Spark-connector-created table is unlistable/unreadable from DuckDB. No table
   PATCH endpoint exists to repair columns (405).
   **Workaround shipped in the notebook:** a separate `interop` schema whose
   tables are written by delta-rs and registered via `POST /tables` with explicit
   `type_precision: 0` — the server then stores and echoes integers, and DuckDB
   reads and appends happily.
4. **Cross-engine verified:** delta-rs writes → DuckDB reads + appends → Spark
   reads all rows and appends its own, all through the same UC table.
5. The official image's healthcheck works via bash `/dev/tcp` (bash present).
   The H2 metadata db needs upstream's `hibernate.hbm2ddl.auto=update` keys or
   the server crash-loops on an empty database.

## Error handling / testing

- Compose healthcheck on port 8080 (TCP), verified live.
- End-to-end test in this session: `just up unity`, REST probe
  (`/api/2.1/unity-catalog/catalogs`), then run the notebook logic headless
  (plain-python mirror) before finalizing the marimo file.
- Failure modes documented in the notebook: DuckDB DDL unsupported; MinIO-backed
  external tables fail at credential vending (upstream limitation).

## Out of scope

- `CatalogType.UNITY` in `spark_connector` and `LakehouseConnection` support.
- UC UI container (`unitycatalog-ui` image is a year stale, no versioned tags).
- Auth (`server.authorization=disable`; note CVE-2026-27478 requires
  `server.allowed-issuers`/`server.audiences` if auth is ever enabled).
