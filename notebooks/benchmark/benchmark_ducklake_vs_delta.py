import marimo

__generated_with = "0.23.1"
app = marimo.App(width="full")


@app.cell
def _():
    """Imports and configuration."""
    import sys
    from pathlib import Path

    import marimo as mo
    from loguru import logger

    logger.remove()
    logger.add(sys.stderr, level="INFO")
    # Add the benchmark directory to sys.path so helpers is importable
    _benchmark_dir = str(Path(__file__).resolve().parent)
    if _benchmark_dir not in sys.path:
        sys.path.insert(0, _benchmark_dir)

    from helpers import (
        BenchmarkResult,
        RunSelection,
        export_results,
        load_config,
        pivot_comparison,
        results_to_dataframe,
        run_benchmark,
        speedup_ratios,
        storage_efficiency,
    )

    CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"
    RESULTS_DIR = str(Path(__file__).resolve().parent / "results")

    config = load_config(CONFIG_PATH)
    mo.md(
        f"# {config.name}\n\n"
        f"Loaded config: **{len(config.table_sizes)}** sizes, "
        f"**{len(config.storage_modes)}** storage modes, "
        f"**{len(config.engines)}** engines, "
        f"**{len(config.operations.all_operations)}** operations"
    )
    return (
        BenchmarkResult,
        RESULTS_DIR,
        RunSelection,
        config,
        export_results,
        logger,
        mo,
        pivot_comparison,
        results_to_dataframe,
        run_benchmark,
        speedup_ratios,
        storage_efficiency,
    )


@app.cell
def _(config, mo):
    """Size selector."""
    size_selector = mo.ui.multiselect(
        options=list(config.table_sizes.keys()),
        value=["tiny", "small"],
        label="Select table sizes to benchmark",
    )
    size_selector
    return (size_selector,)


@app.cell
def _(config, mo):
    """Storage mode selector. Defaults to default_storage_modes from YAML."""
    storage_selector = mo.ui.multiselect(
        options=config.storage_modes,
        value=config.default_storage_modes or config.storage_modes,
        label="Select storage modes",
    )
    storage_selector
    return (storage_selector,)


@app.cell
def _(config, mo):
    """Engine selector."""
    engine_selector = mo.ui.multiselect(
        options=config.engines,
        value=config.engines,
        label="Select engines",
    )
    engine_selector
    return (engine_selector,)


@app.cell
def _(config, mo):
    """Operation selector."""
    all_ops = config.operations.all_operations
    operation_selector = mo.ui.multiselect(
        options=all_ops,
        value=all_ops,
        label="Select operations to benchmark",
    )
    operation_selector
    return (operation_selector,)


@app.cell
def _(mo):
    """Run button."""
    run_button = mo.ui.run_button(label="Run Benchmark")
    run_button
    return (run_button,)


@app.cell
def _(
    BenchmarkResult,
    RunSelection,
    config,
    engine_selector,
    mo,
    operation_selector,
    run_benchmark,
    run_button,
    size_selector,
    storage_selector,
):
    """Main benchmark orchestration — delegates to helpers.runner.run_benchmark."""
    mo.stop(not run_button.value, "Click 'Run Benchmark' to start.")

    selection = RunSelection(
        sizes=list(size_selector.value),
        storage_modes=list(storage_selector.value),
        engines=list(engine_selector.value),
        operations=list(operation_selector.value),
    )
    all_results: list[BenchmarkResult] = run_benchmark(config, selection)
    return (all_results,)


@app.cell
def _(all_results, mo, results_to_dataframe):
    """Raw results table."""
    results_df = results_to_dataframe(all_results)
    mo.md("## Raw Results")
    mo.output.replace(results_df)
    return (results_df,)


@app.cell
def _(mo, pivot_comparison, results_df):
    """Side-by-side comparison."""
    mo.md("## DuckLake vs Delta — Side by Side")
    mo.output.replace(pivot_comparison(results_df))
    return


@app.cell
def _(mo, results_df, speedup_ratios):
    """Speedup ratios."""
    mo.md("## Speedup Ratios (>1 means DuckLake faster)")
    mo.output.replace(speedup_ratios(results_df))
    return


@app.cell
def _(mo, results_df, storage_efficiency):
    """Storage efficiency comparison."""
    mo.md("## Storage Efficiency (data files + Postgres metadata, side by side)")
    mo.output.replace(storage_efficiency(results_df))
    return


@app.cell
def _(RESULTS_DIR, export_results, mo, results_df):
    """Export results to CSV."""
    if not results_df.is_empty():
        csv_path = export_results(results_df, RESULTS_DIR)
        mo.md(f"Results exported to `{csv_path}`")
    return


@app.cell
def _(mo, results_df):
    """Textual analysis report."""
    import polars as pl

    mo.stop(results_df.is_empty(), "_No results to analyze._")

    ducklake = results_df.filter(pl.col("engine") == "ducklake")
    delta = results_df.filter(pl.col("engine") == "delta")
    mo.stop(
        ducklake.is_empty() or delta.is_empty(),
        "_Need results from both engines to generate comparison._",
    )

    lines = ["## Benchmark Analysis Report\n"]
    ops = results_df.select("operation").unique().sort("operation").to_series().to_list()
    storage_modes = results_df.select("storage_mode").unique().to_series().to_list()
    sizes = results_df.select("table_size").unique().to_series().to_list()

    lines.append("### Performance Summary by Operation\n")
    for op in ops:
        lines.append(f"**{op}**\n")
        dl_op = ducklake.filter(pl.col("operation") == op)
        de_op = delta.filter(pl.col("operation") == op)
        if dl_op.is_empty() or de_op.is_empty():
            lines.append("- _Skipped (missing data for one engine)_\n")
            continue
        for sm in storage_modes:
            dl_sm = dl_op.filter(pl.col("storage_mode") == sm)
            de_sm = de_op.filter(pl.col("storage_mode") == sm)
            if dl_sm.is_empty() or de_sm.is_empty():
                continue
            lines.append(f"- **{sm} storage:**")
            for sz in sizes:
                dl_row = dl_sm.filter(pl.col("table_size") == sz)
                de_row = de_sm.filter(pl.col("table_size") == sz)
                if dl_row.is_empty() or de_row.is_empty():
                    continue
                dl_t = dl_row["wall_time_seconds"][0]
                de_t = de_row["wall_time_seconds"][0]
                dl_m = dl_row["peak_rss_mb"][0]
                de_m = de_row["peak_rss_mb"][0]
                dl_d = dl_row["total_storage_mb"][0]
                de_d = de_row["total_storage_mb"][0]
                if dl_t > 0 and de_t > 0:
                    if de_t < dl_t:
                        pct = ((dl_t - de_t) / dl_t) * 100
                        verdict = f"Delta is **{pct:.0f}% faster**"
                    elif dl_t < de_t:
                        pct = ((de_t - dl_t) / de_t) * 100
                        verdict = f"DuckLake is **{pct:.0f}% faster**"
                    else:
                        verdict = "Equal performance"
                else:
                    verdict = "N/A"
                extra = ""
                if dl_m > 0 and de_m > 0:
                    diff = abs(dl_m - de_m)
                    if diff > 1.0:
                        winner = "Delta" if de_m < dl_m else "DuckLake"
                        extra += f", {winner} uses {diff:.0f} MB less peak RSS"
                if dl_d > 0 and de_d > 0:
                    if de_d < dl_d:
                        extra += f", Delta uses {((dl_d - de_d) / dl_d) * 100:.0f}% less total storage"
                    elif dl_d < de_d:
                        extra += f", DuckLake uses {((de_d - dl_d) / de_d) * 100:.0f}% less total storage"
                rc = dl_row["row_count"][0]
                lines.append(f"  - `{sz}` ({rc:,} rows): {verdict} (DuckLake {dl_t:.3f}s vs Delta {de_t:.3f}s{extra})")
        lines.append("")

    lines.append("### Overall Summary\n")
    joined = ducklake.select(
        "operation",
        "storage_mode",
        "table_size",
        pl.col("wall_time_seconds").alias("dl_time"),
    ).join(
        delta.select(
            "operation",
            "storage_mode",
            "table_size",
            pl.col("wall_time_seconds").alias("de_time"),
        ),
        on=["operation", "storage_mode", "table_size"],
        how="inner",
    )
    if not joined.is_empty():
        dl_wins = joined.filter(pl.col("dl_time") < pl.col("de_time")).height
        de_wins = joined.filter(pl.col("de_time") < pl.col("dl_time")).height
        total = joined.height
        lines.extend((
            f"Out of **{total}** comparisons:",
            f"- DuckLake was faster in **{dl_wins}** ({dl_wins / total * 100:.0f}%)",
            f"- Delta was faster in **{de_wins}** ({de_wins / total * 100:.0f}%)",
        ))
        ties = total - dl_wins - de_wins
        if ties:
            lines.append(f"- Tied in **{ties}** ({ties / total * 100:.0f}%)")
        avg_dl = joined["dl_time"].mean()
        avg_de = joined["de_time"].mean()
        if avg_dl and avg_de and avg_dl > 0:
            ratio = avg_de / avg_dl
            if ratio > 1:
                lines.append(f"\nOn average, DuckLake is **{ratio:.2f}x faster** across all operations.")
            else:
                lines.append(f"\nOn average, Delta is **{1 / ratio:.2f}x faster** across all operations.")

    dl_disk = ducklake.filter(
        pl.col("operation").str.starts_with("write_") | pl.col("operation").str.starts_with("merge_")
    )["total_storage_mb"].mean()
    de_disk = delta.filter(
        pl.col("operation").str.starts_with("write_") | pl.col("operation").str.starts_with("merge_")
    )["total_storage_mb"].mean()
    if dl_disk and de_disk and dl_disk > 0 and de_disk > 0:
        lines.append(f"\nAverage write storage: DuckLake **{dl_disk:.1f}MB** vs Delta **{de_disk:.1f}MB**")
        if de_disk < dl_disk:
            lines.append(f"Delta is **{((dl_disk - de_disk) / dl_disk) * 100:.0f}% more storage-efficient**.")
        elif dl_disk < de_disk:
            lines.append(f"DuckLake is **{((de_disk - dl_disk) / de_disk) * 100:.0f}% more storage-efficient**.")

    mo.md("\n".join(lines))
    return


if __name__ == "__main__":
    app.run()
