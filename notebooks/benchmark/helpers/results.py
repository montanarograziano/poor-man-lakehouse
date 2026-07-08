"""Results aggregation, comparison, and export."""

from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime, timezone

import polars as pl
from loguru import logger

from .metrics import BenchmarkResult


def results_to_dataframe(results: list[BenchmarkResult]) -> pl.DataFrame:
    """Convert list of BenchmarkResult to a Polars DataFrame."""
    if not results:
        return pl.DataFrame()
    return pl.DataFrame([asdict(r) for r in results])


def pivot_comparison(df: pl.DataFrame) -> pl.DataFrame:
    """Side-by-side comparison of DuckLake vs Delta on the headline metrics."""
    if df.is_empty():
        return df
    return (
        df.select(
            "operation",
            "storage_mode",
            "table_size",
            "engine",
            "wall_time_seconds",
            "peak_rss_mb",
            "delta_rss_mb",
            "disk_usage_mb",
            "postgres_metadata_mb",
            "total_storage_mb",
            "file_count",
            "throughput_rows_per_sec",
        )
        .pivot(on="engine", index=["operation", "storage_mode", "table_size"])
        .sort("operation", "storage_mode", "table_size")
    )


def speedup_ratios(df: pl.DataFrame) -> pl.DataFrame:
    """Speedup ratios (>1 means DuckLake faster) and memory ratios."""
    if df.is_empty():
        return df

    ducklake = df.filter(pl.col("engine") == "ducklake").select(
        "operation",
        "storage_mode",
        "table_size",
        pl.col("wall_time_seconds").alias("ducklake_time"),
        pl.col("peak_rss_mb").alias("ducklake_peak_rss"),
        pl.col("delta_rss_mb").alias("ducklake_delta_rss"),
    )
    delta = df.filter(pl.col("engine") == "delta").select(
        "operation",
        "storage_mode",
        "table_size",
        pl.col("wall_time_seconds").alias("delta_time"),
        pl.col("peak_rss_mb").alias("delta_peak_rss"),
        pl.col("delta_rss_mb").alias("delta_delta_rss"),
    )

    joined = ducklake.join(delta, on=["operation", "storage_mode", "table_size"], how="inner")
    return joined.with_columns(
        (pl.col("delta_time") / pl.col("ducklake_time")).round(2).alias("time_speedup_ducklake_vs_delta"),
        (pl.col("delta_peak_rss") / pl.col("ducklake_peak_rss")).round(2).alias("peak_rss_ratio_ducklake_vs_delta"),
        (pl.col("delta_delta_rss") / pl.col("ducklake_delta_rss")).round(2).alias("delta_rss_ratio_ducklake_vs_delta"),
    ).sort("operation", "storage_mode", "table_size")


def storage_efficiency(df: pl.DataFrame) -> pl.DataFrame:
    """Compare storage efficiency between engines.

    Shows ``disk_usage_mb`` (data files), ``postgres_metadata_mb`` (catalog), and
    ``total_storage_mb`` (sum) side by side per engine, plus file_count and avg_file_size.
    """
    if df.is_empty():
        return df
    write_ops = df.filter(pl.col("operation").str.starts_with("write_") | pl.col("operation").str.starts_with("merge_"))
    if write_ops.is_empty():
        return write_ops
    return (
        write_ops.select(
            "operation",
            "storage_mode",
            "table_size",
            "engine",
            "disk_usage_mb",
            "postgres_metadata_mb",
            "total_storage_mb",
            "file_count",
            "avg_file_size_mb",
        )
        .pivot(on="engine", index=["operation", "storage_mode", "table_size"])
        .sort("operation", "storage_mode", "table_size")
    )


def export_results(df: pl.DataFrame, output_dir: str) -> str:
    """Export results to a timestamped CSV. Returns the path."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_results_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    df.write_csv(filepath)
    logger.info(f"Results exported to {filepath}")
    return filepath
