r"""Headless CLI runner for the DuckLake vs Delta-rs benchmark.

Usage:
    uv run python -m notebooks.benchmark.run_benchmark \
        --sizes tiny --engines ducklake delta \
        --operations write_append read_full_scan
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Make the helpers package importable when invoked as a script.
_BENCH_DIR = Path(__file__).resolve().parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from helpers import (  # noqa: E402
    RunSelection,
    export_results,
    load_config,
    pivot_comparison,
    results_to_dataframe,
    run_benchmark,
    speedup_ratios,
    storage_efficiency,
)
from loguru import logger  # noqa: E402


def _build_selection(cfg, args: argparse.Namespace) -> RunSelection:  # noqa: ANN001
    sizes = args.sizes or list(cfg.table_sizes.keys())
    storage_modes = args.storage_modes or cfg.default_storage_modes or cfg.storage_modes
    engines = args.engines or list(cfg.engines)
    operations = args.operations or cfg.operations.all_operations
    return RunSelection(
        sizes=sizes,
        storage_modes=storage_modes,
        engines=engines,
        operations=operations,
    )


def main(argv: list[str] | None = None) -> int:
    """Parse CLI args and run the benchmark."""
    parser = argparse.ArgumentParser(description="Headless DuckLake vs Delta-rs benchmark runner.")
    parser.add_argument(
        "--config",
        type=Path,
        default=_BENCH_DIR / "config.yaml",
        help="Path to config.yaml (default: %(default)s)",
    )
    parser.add_argument(
        "--sizes",
        nargs="+",
        help="Table sizes to run (e.g. tiny small medium). Defaults to all.",
    )
    parser.add_argument(
        "--storage-modes",
        nargs="+",
        dest="storage_modes",
        help="Storage modes to run (e.g. local s3). Defaults to default_storage_modes from YAML.",
    )
    parser.add_argument(
        "--engines",
        nargs="+",
        choices=["ducklake", "delta"],
        help="Engines to run. Defaults to all.",
    )
    parser.add_argument(
        "--operations",
        nargs="+",
        help=(
            "Operations to run (write_append, write_overwrite, merge_upsert, "
            "read_full_scan, read_filtered_scan, read_aggregation). Defaults to all."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_BENCH_DIR / "results",
        help="Directory to write CSV results (default: %(default)s)",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Skip CSV export (useful for smoke tests).",
    )
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    sel = _build_selection(cfg, args)

    logger.info(f"Selection: sizes={sel.sizes} storage={sel.storage_modes} engines={sel.engines} ops={sel.operations}")

    results = run_benchmark(cfg, sel)
    df = results_to_dataframe(results)

    if df.is_empty():
        logger.warning("No results collected.")
        return 1

    import polars as pl

    with pl.Config(tbl_rows=200, tbl_cols=20, tbl_width_chars=200):
        print("\n=== Raw results ===")  # noqa: T201
        print(df)  # noqa: T201
        print("\n=== Side-by-side ===")  # noqa: T201
        print(pivot_comparison(df))  # noqa: T201
        print("\n=== Speedup ratios (>1 means DuckLake faster) ===")  # noqa: T201
        print(speedup_ratios(df))  # noqa: T201
        print("\n=== Storage efficiency ===")  # noqa: T201
        print(storage_efficiency(df))  # noqa: T201

    if not args.no_csv:
        path = export_results(df, str(args.output_dir))
        logger.info(f"CSV written to {path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
