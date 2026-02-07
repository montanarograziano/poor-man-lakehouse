"""IPython/Jupyter magic commands for SQL queries against Unity Catalog.

This module provides Databricks-style %%sql cell magic for Jupyter notebooks.

Usage:
    # In a Jupyter notebook cell:
    from poor_man_lakehouse.polars_connector import PolarsClient, load_sql_magic

    client = PolarsClient()
    load_sql_magic(client)

    # Then in any cell:
    %%sql
    SELECT * FROM unity.default.test_table
    WHERE value > 100

    # Or with options:
    %%sql --limit 100 --no-display
    SELECT * FROM unity.default.test_table
"""

from __future__ import annotations

import argparse
import time
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from IPython.core.interactiveshell import InteractiveShell

    from poor_man_lakehouse.polars_connector.client import PolarsClient

# Global client instance
_client: PolarsClient | None = None


def set_client(client: "PolarsClient") -> None:
    """Set the global PolarsClient instance for the magic commands."""
    global _client
    _client = client


def get_client() -> "PolarsClient":
    """Get the global PolarsClient instance, creating one if needed."""
    global _client
    if _client is None:
        from poor_man_lakehouse.polars_connector.client import PolarsClient

        _client = PolarsClient()
    return _client


def _parse_sql_args(line: str) -> argparse.Namespace:
    """Parse the arguments from the %%sql magic line."""
    parser = argparse.ArgumentParser(prog="%%sql", add_help=False)
    parser.add_argument("--limit", "-l", type=int, default=1000, help="Max rows to display (default: 1000)")
    parser.add_argument("--no-limit", action="store_true", help="Disable row limit")
    parser.add_argument("--no-display", "-q", action="store_true", help="Don't display results, just return DataFrame")
    parser.add_argument("--lazy", action="store_true", help="Return LazyFrame instead of DataFrame")
    parser.add_argument("--help", "-h", action="store_true", help="Show help message")

    try:
        args = parser.parse_args(line.split() if line else [])
    except SystemExit:
        args = argparse.Namespace(limit=1000, no_limit=False, no_display=False, lazy=False, help=False)

    return args


def _display_result(df, elapsed: float, limit: int | None, truncated: bool) -> None:
    """Display the query result with metadata."""
    from IPython.display import HTML, display

    row_count = len(df)

    # Build status message
    status_parts = [f"<b>{row_count:,} rows</b>"]
    if truncated:
        status_parts.append(f"(limited to {limit:,})")
    status_parts.append(f"in <b>{elapsed:.2f}s</b>")
    status_msg = " ".join(status_parts)

    # Display status
    display(HTML(f"<div style='color: #666; font-size: 12px; margin-bottom: 8px;'>{status_msg}</div>"))

    # Display the dataframe
    display(df)


def sql_magic(line: str, cell: str) -> "pl.DataFrame | pl.LazyFrame | None":
    """Execute SQL against Unity Catalog and display results.

    This is the implementation of the %%sql cell magic.

    Usage:
        %%sql [--limit N] [--no-limit] [--no-display] [--lazy]
        SELECT * FROM catalog.namespace.table

    Options:
        --limit N, -l N    Max rows to display (default: 1000)
        --no-limit         Disable row limit
        --no-display, -q   Return DataFrame without displaying
        --lazy             Return LazyFrame instead of DataFrame
        --help, -h         Show this help message

    Examples:
        %%sql
        SELECT * FROM unity.default.test_table

        %%sql --limit 50
        SELECT * FROM unity.default.test_table ORDER BY id

        %%sql --no-display
        SELECT COUNT(*) FROM unity.default.test_table
    """
    args = _parse_sql_args(line)

    if args.help:
        print(sql_magic.__doc__)
        return None

    client = get_client()
    query = cell.strip()

    if not query:
        print("Error: No SQL query provided")
        return None

    # Execute the query
    start = time.perf_counter()
    try:
        result = client.sql(query, lazy=args.lazy)
    except Exception as e:
        print(f"SQL Error: {e}")
        return None
    elapsed = time.perf_counter() - start

    # For lazy results, just return
    if args.lazy and isinstance(result, pl.LazyFrame):
        if not args.no_display:
            print(f"LazyFrame created in {elapsed:.2f}s")
            print(result.explain())
        return result

    # Apply limit
    limit = None if args.no_limit else args.limit
    truncated = False
    if limit is not None and isinstance(result, pl.DataFrame) and len(result) > limit:
        result = result.head(limit)
        truncated = True

    # Display or return
    if args.no_display:
        return result

    _display_result(result, elapsed, limit, truncated)
    return result


def load_ipython_extension(ip: "InteractiveShell") -> None:
    """Load the %%sql magic into IPython.

    This is called automatically when using %load_ext or load_sql_magic().
    """
    # Register the cell magic
    ip.register_magic_function(sql_magic, magic_kind="cell", magic_name="sql")  # type: ignore[call-arg,arg-type]

    print("✓ SQL magic loaded. Use %%sql to query Unity Catalog tables.")
    print("  Example:")
    print("    %%sql")
    print("    SELECT * FROM unity.default.test_table")


def unload_ipython_extension(ip: "InteractiveShell") -> None:
    """Unload the %%sql magic from IPython."""
    pass  # IPython handles cleanup automatically
