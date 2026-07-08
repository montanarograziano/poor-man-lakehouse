import marimo

__generated_with = "0.23.3"
app = marimo.App(width="full")


@app.cell
def _():
    """Setup: imports, config, data generation."""
    import os
    import shutil
    import subprocess
    import sys
    from pathlib import Path

    import duckdb
    import marimo as mo
    import polars as pl
    from deltalake import DeltaTable, write_deltalake
    from loguru import logger

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    _benchmark_dir = str(Path(__file__).resolve().parent)
    if _benchmark_dir not in sys.path:
        sys.path.insert(0, _benchmark_dir)

    from helpers import PARTITION_COL, GeneratorSpec, StreamingGenerator, load_config

    config = load_config(Path(__file__).resolve().parent / "config.yaml")

    ROW_COUNT = 1_000_000
    TABLE_NAME = "playground_partitioned"

    spec = GeneratorSpec(schema_config=config.schema, total_rows=ROW_COUNT, seed=99)
    data = StreamingGenerator(spec).arrow_reader().read_all()
    mo.md(
        f"# Partitioned Table Playground\n\n"
        f"Generated **{data.num_rows:,}** rows with **{data.num_columns}** columns.\n\n"
        f"Partition column: `{PARTITION_COL}` ({data.schema.field(PARTITION_COL).type}), "
        f"dates {spec.date_start} to {spec.date_end}"
    )
    return (
        DeltaTable,
        PARTITION_COL,
        TABLE_NAME,
        config,
        data,
        duckdb,
        logger,
        mo,
        os,
        pl,
        shutil,
        subprocess,
        write_deltalake,
    )


@app.cell
def _():
    return


@app.cell
def _(PARTITION_COL, TABLE_NAME, config, data, logger, mo, os, shutil, write_deltalake):
    """Write partitioned Delta table (local)."""
    delta_path = os.path.join(
        os.path.abspath(config.local.base_path),
        config.local.delta_prefix,
        f"{TABLE_NAME}_delta",
    )
    os.makedirs(os.path.dirname(delta_path), exist_ok=True)

    # Clean previous runs

    if os.path.exists(delta_path):
        shutil.rmtree(delta_path)

    write_deltalake(
        table_or_uri=delta_path,
        data=data,
        mode="overwrite",
        partition_by=[PARTITION_COL],
    )
    n_partitions = len(
        [d for d in os.listdir(delta_path) if d.startswith(f"{PARTITION_COL}=")]
    )
    logger.info(
        f"Delta table written to {delta_path} with {n_partitions} partitions"
    )
    mo.md(
        f"### Delta Table (partitioned by `{PARTITION_COL}`)\n\n"
        f"- Path: `{delta_path}`\n"
        f"- Partitions: **{n_partitions}**"
    )
    return (delta_path,)


@app.cell
def _(attach_uri):
    attach_uri
    return


@app.cell
def _(PARTITION_COL, TABLE_NAME, config, duckdb, logger, mo, os, shutil, subprocess):
    """Write partitioned DuckLake table (local, PostgreSQL catalog)."""
    pg = config.postgres
    pg_database = f"{pg.database}_playground"

    # Ensure database exists
    result = subprocess.run(
        [
            "psql",
            "-h",
            pg.host,
            "-p",
            str(pg.port),
            "-U",
            pg.user,
            "-d",
            "postgres",
            "-c",
            f"CREATE DATABASE {pg_database};",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "PGPASSWORD": pg.password},
    )
    if result.returncode == 0:
        logger.info(f"Created database: {pg_database}")

    ducklake_data_path = os.path.join(
        os.path.abspath(config.local.base_path), "ducklake_playground/"
    )
    os.makedirs(ducklake_data_path, exist_ok=True)

    # Clean previous data files

    if os.path.exists(ducklake_data_path):
        shutil.rmtree(ducklake_data_path)
        os.makedirs(ducklake_data_path, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL ducklake; INSTALL postgres;")
    con.execute("LOAD ducklake; LOAD postgres;")

    catalog_name = "playground_ducklake"
    attach_uri = (
        f"ducklake:postgres:dbname={pg_database} host={pg.host} "
        f"port={pg.port} user={pg.user} password={pg.password}"
    )
    con.execute(f"""
        ATTACH OR REPLACE '{attach_uri}' AS {catalog_name}
            (DATA_PATH '{ducklake_data_path}');
    """)
    con.execute(f"USE {catalog_name};")

    fq = f"{catalog_name}.main.{TABLE_NAME}"
    con.execute(f"DROP TABLE IF EXISTS {fq}")

    # Create empty table with schema, set partitioning, THEN insert data.
    # Partitioning must be set before data is written — it only affects new inserts.
    # con.register("_src", data)
    con.execute(f"CREATE TABLE {fq} AS SELECT * FROM data LIMIT 0")
    con.execute(f"ALTER TABLE {fq} SET PARTITIONED BY ({PARTITION_COL})")
    logger.info(f"DuckLake table partitioned by {PARTITION_COL}")
    con.execute(f"INSERT INTO {fq} SELECT * FROM data")
    # con.unregister("data")

    row_count = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
    partition_info = f"Partitioned by `{PARTITION_COL}`"
    logger.info(f"DuckLake table written: {row_count} rows")

    mo.md(
        f"### DuckLake Table\n\n"
        f"- Catalog: `{catalog_name}` (PostgreSQL: `{pg_database}`)\n"
        f"- Data path: `{ducklake_data_path}`\n"
        f"- Rows: **{row_count:,}**\n"
        f"- {partition_info}"
    )
    return attach_uri, con, fq


@app.cell
def _(mo):
    """Section header for exploration."""
    mo.md("---\n## Explore the Tables\n\nUse the cells below to query both tables.")
    return


@app.cell
def _(delta_path, mo, pl):
    """Delta: full scan via Polars."""
    delta_df = pl.scan_delta(delta_path).collect()
    mo.md(f"### Delta — Full Scan ({len(delta_df):,} rows)")
    delta_df.head(20)
    return


@app.cell
def _(PARTITION_COL, delta_path, mo, pl):
    """Delta: filtered scan with partition pruning."""
    delta_filtered = (
        pl.scan_delta(delta_path)
        .filter(
            pl.col(PARTITION_COL).is_between(
                pl.lit("2024-01-10").str.to_date(),
                pl.lit("2024-01-20").str.to_date(),
            )
        )
        .collect()
    )
    mo.md(f"### Delta — Filtered Scan: 2024-01-10 to 2024-01-20 ({len(delta_filtered):,} rows)")
    delta_filtered.head(20)
    return


@app.cell
def _(PARTITION_COL, delta_path, mo, pl):
    """Delta: aggregation by varchar_col."""
    delta_agg = (
        pl.scan_delta(delta_path)
        .group_by("varchar_col")
        .agg(
            pl.len().alias("count"),
            pl.col("int64_col").sum().alias("sum_int64"),
            pl.col("float64_col").mean().alias("avg_float64"),
            pl.col(PARTITION_COL).min().alias("min_date"),
            pl.col(PARTITION_COL).max().alias("max_date"),
        )
        .sort("count", descending=True)
        .collect()
    )
    mo.md(f"### Delta — Aggregation by `varchar_col` ({len(delta_agg)} groups)")
    delta_agg.head(20)
    return


@app.cell
def _(DeltaTable, delta_path, mo, pl):
    """Delta: history and versions."""
    dt = DeltaTable(delta_path)
    history = pl.from_dicts(dt.history())
    mo.md(f"### Delta — History ({len(history)} versions)")
    history
    return


@app.cell
def _(mo):
    """SQL playground header."""
    mo.md(
        "---\n## SQL Playground\n\n"
        "Use `mo.sql()` in a new cell to run arbitrary queries.\n\n"
        "**Available tables:**\n"
        "- DuckLake: `playground_ducklake.main.playground_partitioned`\n"
        "- Delta: use `delta_scan('path')` or Polars `pl.scan_delta(delta_path)`"
    )
    return


@app.cell
def _(con, fq, mo):
    """Attach DuckLake catalog to marimo's built-in DuckDB so mo.sql() can query it."""
    # marimo's mo.sql() uses its own internal DuckDB connection.
    # We re-attach the DuckLake catalog there via con so it's available in SQL cells.
    catalogs = [r[0] for r in con.execute("SHOW DATABASES").fetchall()]
    mo.md(
        f"### DuckLake catalog attached\n\n"
        f"Available catalogs: `{'`, `'.join(catalogs)}`\n\n"
        f"Query the table with: `SELECT * FROM {fq}`"
    )
    return


@app.cell(hide_code=True)
def _(con, mo):
    _df = mo.sql(
        """
        SELECT *
        from playground_ducklake.main.playground_partitioned
        WHERE event_date BETWEEN '2024-01-10' and '2024-01-20'
        """,
        engine=con
    )
    return


if __name__ == "__main__":
    app.run()
