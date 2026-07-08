"""Benchmark orchestration: single source of truth consumed by marimo, Jupyter, and CLI.

Key design choices:

* Streaming end-to-end. Every benchmark iteration instantiates a fresh ``StreamingGenerator``
  and hands its ``arrow_reader()`` to the engine. Memory is bounded by ``chunk_size``,
  independent of ``total_rows``.
* One Delta version / one DuckLake snapshot per write call. The engine does the streaming;
  the orchestrator does not loop append-per-batch.
* Read benchmarks: write ONCE, read ``warmup + repeat`` times. Avoids 4x rewriting the table
  for every read op.
"""

from __future__ import annotations

import statistics
from typing import TYPE_CHECKING

from loguru import logger

from .data_generator import GeneratorSpec, StreamingGenerator
from .engines import get_engine
from .metrics import BenchmarkResult, TimingResult, build_result, measure_time_and_memory

if TYPE_CHECKING:
    from .config import BenchmarkConfig, RunSelection


_WRITE_OPS = ("write_append", "write_overwrite")
_MERGE_OPS = ("merge_upsert",)
_READ_OPS = ("read_full_scan", "read_filtered_scan", "read_aggregation")


def _validate_selection(cfg: BenchmarkConfig, sel: RunSelection) -> None:
    """Catch typos and unknown values up front, before doing any work."""
    bad_sizes = [s for s in sel.sizes if s not in cfg.table_sizes]
    if bad_sizes:
        msg = f"Unknown sizes: {bad_sizes}. Available: {list(cfg.table_sizes.keys())}"
        raise ValueError(msg)
    bad_storage = [s for s in sel.storage_modes if s not in cfg.storage_modes]
    if bad_storage:
        msg = f"Unknown storage modes: {bad_storage}. Available: {cfg.storage_modes}"
        raise ValueError(msg)
    bad_engines = [e for e in sel.engines if e not in cfg.engines]
    if bad_engines:
        msg = f"Unknown engines: {bad_engines}. Available: {cfg.engines}"
        raise ValueError(msg)
    bad_ops = [
        o
        for o in sel.operations
        if o not in _WRITE_OPS + _MERGE_OPS + _READ_OPS and o not in cfg.operations.all_operations
    ]
    if bad_ops:
        msg = f"Unknown operations: {bad_ops}. Available: {cfg.operations.all_operations}"
        raise ValueError(msg)


def _make_generator(cfg: BenchmarkConfig, n_rows: int) -> StreamingGenerator:
    """Fresh, single-use streaming generator for one benchmark iteration."""
    return StreamingGenerator(
        GeneratorSpec(
            schema_config=cfg.schema,
            total_rows=n_rows,
            chunk_size=cfg.batch_size,
            seed=cfg.schema.seed,
        )
    )


def _median_timing(timings: list[TimingResult]) -> TimingResult:
    return TimingResult(
        wall_time_seconds=statistics.median(t.wall_time_seconds for t in timings),
        peak_rss_mb=statistics.median(t.peak_rss_mb for t in timings),
        delta_rss_mb=statistics.median(t.delta_rss_mb for t in timings),
    )


def run_benchmark(cfg: BenchmarkConfig, sel: RunSelection) -> list[BenchmarkResult]:
    """Run the benchmark and return collected results.

    Args:
        cfg: Loaded config (typically from ``load_config(...)``).
        sel: Concrete subset of sizes/storage/engines/operations to run.

    Returns:
        List of ``BenchmarkResult``; one per (size, storage, engine, operation) combo.
    """
    _validate_selection(cfg, sel)

    write_ops = [o for o in sel.operations if o in _WRITE_OPS]
    merge_ops = [o for o in sel.operations if o in _MERGE_OPS]
    read_ops = [o for o in sel.operations if o in _READ_OPS]

    total_combos = (
        len(sel.sizes) * len(sel.storage_modes) * len(sel.engines) * (len(write_ops) + len(merge_ops) + len(read_ops))
    )
    logger.info(f"Starting benchmark: {total_combos} combinations")
    results: list[BenchmarkResult] = []
    full_scan_times: dict[tuple[str, str, str], float] = {}

    for size_name in sel.sizes:
        n_rows = cfg.table_sizes[size_name]
        logger.info(f"=== Table size: {size_name} ({n_rows:,} rows) ===")

        for storage_mode in sel.storage_modes:
            for engine_name in sel.engines:
                _run_engine(
                    cfg=cfg,
                    sel=sel,
                    size_name=size_name,
                    n_rows=n_rows,
                    storage_mode=storage_mode,
                    engine_name=engine_name,
                    write_ops=write_ops,
                    merge_ops=merge_ops,
                    read_ops=read_ops,
                    results=results,
                    full_scan_times=full_scan_times,
                )

    logger.info(f"Benchmark complete: {len(results)} results collected")
    return results


def _run_engine(
    *,
    cfg: BenchmarkConfig,
    sel: RunSelection,  # noqa: ARG001 - kept for symmetry / future use
    size_name: str,
    n_rows: int,
    storage_mode: str,
    engine_name: str,
    write_ops: list[str],
    merge_ops: list[str],
    read_ops: list[str],
    results: list[BenchmarkResult],
    full_scan_times: dict[tuple[str, str, str], float],
) -> None:
    """Run all selected ops for one (size, storage, engine) tuple."""
    # --- Write + merge ops: full setup-write-teardown per repeat ---
    for op in write_ops + merge_ops:
        engine = get_engine(engine_name)
        timings: list[TimingResult] = []
        disk_b, file_n, pg_b = 0, 0, 0
        total_runs = cfg.warmup_runs + cfg.repeat_runs
        for run_idx in range(total_runs):
            is_warmup = run_idx < cfg.warmup_runs
            engine.setup(cfg, storage_mode)
            try:
                logger.info(
                    f"  [{engine_name}] {op} on {storage_mode} ({size_name}) "
                    f"run {run_idx + 1}/{total_runs}"
                    f"{' [warmup]' if is_warmup else ''}"
                )
                with measure_time_and_memory() as t:
                    gen = _make_generator(cfg, n_rows)
                    if op == "write_append":
                        engine.write_append("bench_table", gen.arrow_reader(), gen.schema)
                    elif op == "write_overwrite":
                        engine.write_overwrite("bench_table", gen.arrow_reader(), gen.schema)
                    elif op == "merge_upsert":
                        # Pre-populate the target, then merge a fresh source generator.
                        engine.write_overwrite("bench_table", gen.arrow_reader(), gen.schema)
                        merge_gen = _make_generator(cfg, n_rows)
                        engine.merge_upsert(
                            "bench_table",
                            merge_gen.merge_arrow_reader(cfg.schema.merge_overlap_ratio),
                            cfg.schema.id_col,
                        )
                if not is_warmup:
                    timings.append(t[0])
                    if run_idx == total_runs - 1:
                        disk_b, file_n = engine.get_disk_usage("bench_table")
                        pg_b = engine.get_postgres_metadata_size("bench_table")
            except Exception:
                logger.exception(f"FAILED: {engine_name}/{op}/{storage_mode}/{size_name} run {run_idx + 1}")
            finally:
                engine.teardown("bench_table")
                engine.close()

        if timings:
            median = _median_timing(timings)
            result = build_result(
                engine=engine_name,
                operation=op,
                storage_mode=storage_mode,
                table_size=size_name,
                row_count=n_rows,
                timing=median,
                disk_bytes=disk_b,
                file_count=file_n,
                postgres_bytes=pg_b,
            )
            results.append(result)
            logger.info(
                f"    OK: {median.wall_time_seconds:.3f}s | "
                f"peak {median.peak_rss_mb:.0f} MB (delta {median.delta_rss_mb:.0f} MB) | "
                f"disk {disk_b / 1024 / 1024:.1f} MB | pg {pg_b / 1024 / 1024:.2f} MB"
            )

    # --- Read ops: write ONCE, then read warmup+repeat times ---
    if not read_ops:
        return

    engine = get_engine(engine_name)
    engine.setup(cfg, storage_mode)
    try:
        # Single populate, no measurement.
        gen = _make_generator(cfg, n_rows)
        engine.write_overwrite("bench_table", gen.arrow_reader(), gen.schema)

        for op in read_ops:
            logger.info(f"  [{engine_name}] {op} on {storage_mode} ({size_name})")
            timings = []
            total_runs = cfg.warmup_runs + cfg.repeat_runs
            for run_idx in range(total_runs):
                is_warmup = run_idx < cfg.warmup_runs
                try:
                    with measure_time_and_memory() as t:
                        getattr(engine, op)("bench_table")
                    if not is_warmup:
                        timings.append(t[0])
                except Exception:
                    logger.exception(f"FAILED: {engine_name}/{op}/{storage_mode}/{size_name} run {run_idx + 1}")

            if not timings:
                continue
            median = _median_timing(timings)
            disk_b, file_n = engine.get_disk_usage("bench_table")
            pg_b = engine.get_postgres_metadata_size("bench_table")

            full_scan_time: float | None = None
            key = (engine_name, storage_mode, size_name)
            if op == "read_full_scan":
                full_scan_times[key] = median.wall_time_seconds
            elif op == "read_filtered_scan":
                full_scan_time = full_scan_times.get(key)

            result = build_result(
                engine=engine_name,
                operation=op,
                storage_mode=storage_mode,
                table_size=size_name,
                row_count=n_rows,
                timing=median,
                disk_bytes=disk_b,
                file_count=file_n,
                postgres_bytes=pg_b,
                full_scan_time=full_scan_time,
            )
            results.append(result)
            logger.info(
                f"    OK: {median.wall_time_seconds:.3f}s | "
                f"peak {median.peak_rss_mb:.0f} MB (delta {median.delta_rss_mb:.0f} MB) | "
                f"disk {disk_b / 1024 / 1024:.1f} MB | pg {pg_b / 1024 / 1024:.2f} MB"
            )
    finally:
        engine.teardown("bench_table")
        engine.close()
