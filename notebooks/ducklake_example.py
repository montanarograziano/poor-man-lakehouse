"""DuckLake playground (marimo).

Attaches to whatever the benchmark already wrote and runs ad-hoc queries with timing + RSS
readouts. Does NOT regenerate data: if the selected table doesn't exist, run the benchmark
first.

Usage:
    1. Run the benchmark (CLI, Jupyter, or marimo) at any size — e.g.
       `uv run python -m notebooks.benchmark.run_benchmark --sizes tiny --operations write_overwrite`
    2. Open this playground:
       `uv run marimo edit notebooks/ducklake_example.py`
"""

import marimo

__generated_with = "0.23.3"
app = marimo.App(width="full")


@app.cell
def _():
    """Imports + path setup."""
    import sys
    from pathlib import Path

    import marimo as mo

    bench_dir = (Path(__file__).resolve().parent / "benchmark").resolve()
    if str(bench_dir) not in sys.path:
        sys.path.insert(0, str(bench_dir))

    from helpers import load_config, measure_time_and_memory
    from helpers.engines.ducklake_engine import DuckLakeEngine

    config = load_config(bench_dir / "config.yaml")
    return DuckLakeEngine, config, measure_time_and_memory, mo


@app.cell
def _(config, mo):
    """Storage mode selector."""
    storage_selector = mo.ui.dropdown(
        options=config.storage_modes,
        value=(config.default_storage_modes or config.storage_modes)[0],
        label="Storage mode",
    )
    storage_selector
    return (storage_selector,)


@app.cell
def _(DuckLakeEngine, config, storage_selector):
    """Attach to the catalog the benchmark created. The engine sets up DuckDB + Postgres + secrets."""
    engine = DuckLakeEngine()
    engine.setup(config, storage_selector.value)
    con = engine._con
    catalog_name = engine._catalog_name
    return catalog_name, con, engine


@app.cell
def _(catalog_name, con, mo):
    """List existing tables in the attached catalog."""
    rows = con.execute(f"SELECT table_name FROM {catalog_name}.information_schema.tables WHERE table_schema = 'main'").fetchall()
    table_names = [r[0] for r in rows]
    if not table_names:
        mo.md(
            "**No tables found.** Run the benchmark first to populate data:\n\n"
            "```bash\nuv run python -m notebooks.benchmark.run_benchmark "
            "--sizes tiny --operations write_overwrite\n```"
        )
    return (table_names,)


@app.cell
def _(mo, table_names):
    """Table selector."""
    mo.stop(not table_names, "_No tables to query yet._")
    table_selector = mo.ui.dropdown(options=table_names, value=table_names[0], label="Table")
    table_selector
    return (table_selector,)


@app.cell
def _(catalog_name, con, mo, table_selector):
    """Schema preview."""
    mo.stop(table_selector.value is None, "_Select a table._")
    fq = f"{catalog_name}.main.{table_selector.value}"
    schema_df = con.execute(f"DESCRIBE {fq}").pl()
    mo.md(f"### Schema: `{fq}`")
    mo.output.append(schema_df)
    return (fq,)


@app.cell
def _(fq, mo):
    """SQL editor. Default query is a partition-pruned scan."""
    default_sql = (
        f"SELECT *\n"
        f"FROM {fq}\n"
        f"WHERE event_date BETWEEN '2024-06-01' AND '2024-12-31'\n"
        f"  AND varchar_col IN ('value_001', 'value_002', 'value_003')\n"
        f"LIMIT 100"
    )
    sql_editor = mo.ui.code_editor(value=default_sql, language="sql", label="SQL query")
    sql_editor
    return (sql_editor,)


@app.cell
def _(mo):
    """Run button (avoid running heavy queries on every cell edit)."""
    run_btn = mo.ui.run_button(label="Run query")
    run_btn
    return (run_btn,)


@app.cell
def _(con, measure_time_and_memory, mo, run_btn, sql_editor):
    """Execute SQL with timing + RSS measurement. Show preview, plan, and metrics."""
    mo.stop(not run_btn.value, "_Click 'Run query' to execute._")
    sql = sql_editor.value

    with measure_time_and_memory() as t:
        result_arrow = con.execute(sql).fetch_arrow_table()
    timing = t[0]

    explain = con.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
    explain_text = "\n".join(row[1] for row in explain)

    import polars as pl

    df = pl.from_arrow(result_arrow)
    preview = df.head(1000)

    metrics_md = (
        f"**Wall time:** {timing.wall_time_seconds:.3f} s &nbsp;|&nbsp; "
        f"**Peak RSS:** {timing.peak_rss_mb:.1f} MB &nbsp;|&nbsp; "
        f"**Delta RSS:** {timing.delta_rss_mb:.1f} MB &nbsp;|&nbsp; "
        f"**Rows returned:** {result_arrow.num_rows:,}"
    )
    mo.output.replace(mo.md(f"### Metrics\n{metrics_md}\n\n### Preview (first 1,000 rows)"))
    mo.output.append(preview)
    mo.output.append(mo.md(f"### EXPLAIN ANALYZE\n```\n{explain_text}\n```"))
    return


@app.cell
def _(fq, mo):
    """Pre-canned demo queries — fill the SQL editor by clicking a button.

    These mirror the benchmark's read operations so you can reproduce numbers interactively.
    """
    full_scan = f"SELECT * FROM {fq}"
    filtered_scan = (
        f"SELECT * FROM {fq}\n"
        f"WHERE event_date BETWEEN '2024-06-01' AND '2024-12-31'\n"
        f"  AND varchar_col IN ('value_001', 'value_002', 'value_003')"
    )
    aggregation = (
        f"SELECT varchar_col, COUNT(*) AS cnt, SUM(int64_col) AS sum_val,\n"
        f"       AVG(float64_col) AS avg_val,\n"
        f"       MIN(event_date) AS min_date, MAX(event_date) AS max_date\n"
        f"FROM {fq}\n"
        f"GROUP BY varchar_col"
    )
    mo.md(
        f"### Pre-canned queries\n\n"
        f"Copy any of these into the SQL editor above:\n\n"
        f"**Full scan**\n```sql\n{full_scan}\n```\n\n"
        f"**Filtered scan (partition prune + value filter)**\n```sql\n{filtered_scan}\n```\n\n"
        f"**Aggregation**\n```sql\n{aggregation}\n```"
    )
    return


@app.cell
def _(engine):
    """Cleanup hook: close the engine when the notebook is closed."""
    import atexit

    atexit.register(engine.close)
    return


if __name__ == "__main__":
    app.run()
