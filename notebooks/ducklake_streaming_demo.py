"""DuckLake streaming demo (marimo).

Equivalent of `ducklake_streaming_demo.ipynb` but written for marimo: queries are native
`mo.sql(...)` cells running against the engine's DuckDB connection. Setup and the streaming
write stay in Python because they need a `pa.RecordBatchReader` and timing instrumentation.

Edit the **Parameters** cell, run all cells, then iterate on the **Query** cell.
"""

import marimo

__generated_with = "0.23.3"
app = marimo.App(width="full")


@app.cell
def _():
    """Imports and config."""
    import sys
    from pathlib import Path

    import marimo as mo

    bench_dir = (Path(__file__).resolve().parent / "benchmark").resolve()
    if str(bench_dir) not in sys.path:
        sys.path.insert(0, str(bench_dir))

    from helpers import (
        GeneratorSpec,
        StreamingGenerator,
        load_config,
        measure_time_and_memory,
    )
    from helpers.engines.ducklake_engine import DuckLakeEngine

    config = load_config(bench_dir / "config.yaml")
    return (
        DuckLakeEngine,
        GeneratorSpec,
        StreamingGenerator,
        config,
        measure_time_and_memory,
        mo,
    )


@app.cell
def _(config, mo):
    """Parameters. Re-run downstream cells after changing these."""
    row_count = mo.ui.number(
        value=100_000_000, start=1_000, step=10_000, label="Row count"
    )
    table_name = mo.ui.text(value="demo_table", label="Table name")
    storage_mode = mo.ui.dropdown(
        options=config.storage_modes,
        value=(config.default_storage_modes or config.storage_modes)[0],
        label="Storage mode",
    )
    chunk_size = mo.ui.number(
        value=config.batch_size, start=250_000, step=10_000, label="Chunk size"
    )
    mo.vstack([row_count, table_name, storage_mode, chunk_size])
    return chunk_size, row_count, storage_mode, table_name


@app.cell
def _(DuckLakeEngine, config, mo, storage_mode):
    """Attach to the existing DuckLake catalog.

    Reuses the benchmark engine's setup (installs extensions, creates secrets, ATTACHes the
    Postgres-backed catalog at the configured data_path). The engine's DuckDB connection is
    exposed as `con` so subsequent `mo.sql(...)` cells can use it via `engine=con`.
    """
    engine = DuckLakeEngine()
    engine.setup(config, storage_mode.value)
    con = engine._con
    catalog = engine._catalog_name
    mo.md(
        f"Attached **{catalog}** &nbsp;|&nbsp; storage=`{storage_mode.value}` &nbsp;|&nbsp; "
        f"data_path=`{engine._data_path}`"
    )
    return catalog, con, engine


@app.cell
def _(catalog, table_name):
    """Fully qualified table name used by all SQL cells."""
    fq = f"{catalog}.main.{table_name.value}"
    fq
    return (fq,)


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        call ducklake_merge_adjacent_files('bench_ducklake_local');
        call bench_ducklake_local.set_option('parquet_version', 2);
        call bench_ducklake_local.set_option('parquet_compression', 'zstd');
        call bench_ducklake_local.set_option('parquet_row_group_size_bytes', '80MB');
        """,
        engine=con
    )
    return


@app.cell
def _(
    GeneratorSpec,
    StreamingGenerator,
    chunk_size,
    config,
    measure_time_and_memory,
    mo,
    row_count,
):
    """Stream-generate + write. Wrapped in `measure_time_and_memory` for wall time / RSS.

    The writer consumes a `pa.RecordBatchReader` from the StreamingGenerator and produces
    one DuckLake snapshot. No full materialization in Python.
    """
    gen = StreamingGenerator(
        GeneratorSpec(
            schema_config=config.schema,
            total_rows=int(row_count.value),
            chunk_size=int(chunk_size.value),
            seed=config.schema.seed,
        )
    )
    with measure_time_and_memory() as _t:
        pass
        # engine.write_overwrite(table_name.value, gen.arrow_reader(), gen.schema)
    timing = _t[0]
    mo.md(
        f"Wrote **{int(row_count.value):,}** rows in **{timing.wall_time_seconds:.2f}s** "
        f"&nbsp;|&nbsp; peak RSS **{timing.peak_rss_mb:.0f} MB** "
        f"(delta **{timing.delta_rss_mb:.0f} MB**)"
    )
    return


@app.cell(hide_code=True)
def _(con, fq, mo):
    _df = mo.sql(
        f"""
        DESCRIBE {fq}
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(con, mo):
    _df = mo.sql(
        """
        SELECT * FROM ducklake_snapshots('bench_ducklake_local')
        ORDER by snapshot_id desc
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        """
        SELECT *
        FROM bench_ducklake_local.main.demo_table AT (VERSION => 63)
        WHERE event_date BETWEEN '2024-01-01' and '2024-01-10' and int8_col < 2
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(con, fq, mo):
    _df = mo.sql(
        f"""
        SELECT COUNT(*)         AS n,
               MIN(event_date)  AS min_date,
               MAX(event_date)  AS max_date,
               COUNT(DISTINCT event_date) AS distinct_partitions
        FROM {fq}
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(con, fq, mo):
    _df = mo.sql(
        f"""
        SELECT varchar_col,
               COUNT(*)            AS cnt,
               SUM(int64_col)      AS sum_val,
               AVG(float64_col)    AS avg_val,
               MIN(event_date)     AS min_date,
               MAX(event_date)     AS max_date
        FROM {fq}
        WHERE event_date BETWEEN DATE '2024-01-10' AND DATE '2024-01-20'
        GROUP BY varchar_col
        ORDER BY cnt DESC
        LIMIT 20
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(con, fq, mo):
    _df = mo.sql(
        f"""
        EXPLAIN ANALYZE
        SELECT varchar_col, COUNT(*) AS cnt, AVG(float64_col) AS avg_val
        FROM {fq}
        WHERE event_date BETWEEN DATE '2024-01-10' AND DATE '2024-01-20'
        GROUP BY varchar_col
        """,
        engine=con
    )
    return


@app.cell
def _(engine):
    """Cleanup on notebook close. Leaves the table in place so you can keep iterating;
    uncomment the teardown line to drop it.
    """
    import atexit

    # atexit.register(lambda: engine.teardown(table_name.value))
    atexit.register(engine.close)
    return


if __name__ == "__main__":
    app.run()
