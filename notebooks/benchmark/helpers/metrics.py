"""Metric collection utilities for benchmarking.

Memory is measured via process RSS sampled in a background thread. This captures C++/Rust
allocations from DuckDB, delta-rs, and Polars (which ``tracemalloc`` does not see). RSS includes
mmap-backed file pages, so for read-heavy workloads it can include OS-level page cache. See the
companion assumptions document for details.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import psutil
from loguru import logger


@dataclass
class BenchmarkResult:
    """Single benchmark measurement.

    Storage breakdown is split so each contribution stays visible:
        - ``disk_usage_mb``: data files (Parquet, Delta log, DuckLake data files).
        - ``postgres_metadata_mb``: DuckLake catalog metadata size; 0 for engines without one.
        - ``total_storage_mb``: ``disk_usage_mb + postgres_metadata_mb``.
    """

    engine: str
    operation: str
    storage_mode: str
    table_size: str
    row_count: int
    wall_time_seconds: float
    peak_rss_mb: float
    delta_rss_mb: float
    disk_usage_mb: float
    postgres_metadata_mb: float
    total_storage_mb: float
    file_count: int
    throughput_rows_per_sec: float
    avg_file_size_mb: float
    scan_efficiency_ratio: float | None = None


@dataclass
class TimingResult:
    """Raw timing + RSS measurement from a single run."""

    wall_time_seconds: float
    peak_rss_mb: float
    delta_rss_mb: float


@contextmanager
def measure_time_and_memory(sample_interval_s: float = 0.05) -> Iterator[list[TimingResult]]:
    """Measure wall time and peak process RSS over the wrapped block.

    Args:
        sample_interval_s: How often the background thread samples RSS. Tighten for very fast
            operations; default 50 ms is fine for most benchmark ops.

    Yields:
        A single-element list which gets populated with one ``TimingResult`` on exit.
    """
    proc = psutil.Process(os.getpid())
    baseline = proc.memory_info().rss
    peak = baseline
    stop_evt = threading.Event()

    def _sample() -> None:
        nonlocal peak
        while not stop_evt.is_set():
            try:
                rss = proc.memory_info().rss
            except psutil.NoSuchProcess:
                return
            if rss > peak:
                peak = rss
            stop_evt.wait(sample_interval_s)

    container: list[TimingResult] = []
    sampler = threading.Thread(target=_sample, daemon=True)
    start = time.perf_counter()
    sampler.start()
    try:
        yield container
    finally:
        elapsed = time.perf_counter() - start
        stop_evt.set()
        sampler.join()
        container.append(
            TimingResult(
                wall_time_seconds=elapsed,
                peak_rss_mb=peak / (1024 * 1024),
                delta_rss_mb=(peak - baseline) / (1024 * 1024),
            )
        )
        logger.debug(
            f"Elapsed: {elapsed:.3f}s | Peak RSS: {peak / (1024 * 1024):.1f} MB "
            f"| Delta RSS: {(peak - baseline) / (1024 * 1024):.1f} MB"
        )


def get_local_disk_usage(path: str) -> tuple[int, int]:
    """Get total bytes and file count for a local directory.

    Args:
        path: Directory path to measure.

    Returns:
        Tuple of (total_bytes, file_count).
    """
    total_bytes = 0
    file_count = 0
    target = os.path.abspath(path)
    if not os.path.exists(target):
        return 0, 0
    for dirpath, _dirnames, filenames in os.walk(target):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total_bytes += os.path.getsize(fp)
                file_count += 1
    return total_bytes, file_count


def get_s3_disk_usage(*, bucket: str, prefix: str, endpoint: str, access_key: str, secret_key: str) -> tuple[int, int]:
    """Get total bytes and file count for an S3 prefix using DuckDB's httpfs."""
    import duckdb

    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE SECRET s3_measure (
            TYPE S3,
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{endpoint.replace("http://", "").replace("https://", "")}',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)
    s3_path = f"s3://{bucket}/{prefix}**"
    try:
        result = con.execute(f"""
            SELECT COALESCE(SUM(size), 0) AS total_bytes, COUNT(*) AS file_count
            FROM glob('{s3_path}')
        """).fetchone()
        con.close()
        if result:
            return int(result[0]), int(result[1])
    except Exception:
        logger.warning(f"Could not measure S3 disk usage for {s3_path}")
        con.close()
    return 0, 0


def _get_s3_client(*, endpoint: str, access_key: str, secret_key: str):  # noqa: ANN202
    """Create a botocore S3 client with explicit credentials (no env/profile leakage)."""
    import botocore.session

    session = botocore.session.get_session()
    return session.create_client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def s3_rm_recursive(*, bucket: str, prefix: str, endpoint: str, access_key: str, secret_key: str) -> int:
    """Delete all objects under an S3 prefix. Returns count deleted."""
    client = _get_s3_client(endpoint=endpoint, access_key=access_key, secret_key=secret_key)
    deleted = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue
        delete_request = {"Objects": [{"Key": obj["Key"]} for obj in objects]}
        client.delete_objects(Bucket=bucket, Delete=delete_request)
        deleted += len(objects)
    return deleted


def build_result(
    *,
    engine: str,
    operation: str,
    storage_mode: str,
    table_size: str,
    row_count: int,
    timing: TimingResult,
    disk_bytes: int,
    file_count: int,
    postgres_bytes: int = 0,
    full_scan_time: float | None = None,
) -> BenchmarkResult:
    """Build a ``BenchmarkResult`` from raw measurements."""
    disk_mb = disk_bytes / (1024 * 1024)
    pg_mb = postgres_bytes / (1024 * 1024)
    avg_file_mb = disk_mb / file_count if file_count > 0 else 0.0
    throughput = row_count / timing.wall_time_seconds if timing.wall_time_seconds > 0 else 0.0
    scan_eff = None
    if full_scan_time is not None and full_scan_time > 0:
        scan_eff = timing.wall_time_seconds / full_scan_time

    return BenchmarkResult(
        engine=engine,
        operation=operation,
        storage_mode=storage_mode,
        table_size=table_size,
        row_count=row_count,
        wall_time_seconds=round(timing.wall_time_seconds, 4),
        peak_rss_mb=round(timing.peak_rss_mb, 2),
        delta_rss_mb=round(timing.delta_rss_mb, 2),
        disk_usage_mb=round(disk_mb, 2),
        postgres_metadata_mb=round(pg_mb, 2),
        total_storage_mb=round(disk_mb + pg_mb, 2),
        file_count=file_count,
        throughput_rows_per_sec=round(throughput, 1),
        avg_file_size_mb=round(avg_file_mb, 2),
        scan_efficiency_ratio=round(scan_eff, 4) if scan_eff is not None else None,
    )
