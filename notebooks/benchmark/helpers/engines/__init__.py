"""Benchmark engine implementations."""

from __future__ import annotations

from .base import BenchmarkEngine
from .delta_engine import DeltaEngine
from .ducklake_engine import DuckLakeEngine

_ENGINES: dict[str, type[BenchmarkEngine]] = {
    "ducklake": DuckLakeEngine,
    "delta": DeltaEngine,
}


def get_engine(name: str) -> BenchmarkEngine:
    """Create an engine instance by name.

    Args:
        name: Engine name ("ducklake" or "delta").

    Returns:
        Engine instance.

    Raises:
        ValueError: If engine name is not recognized.
    """
    cls = _ENGINES.get(name)
    if cls is None:
        msg = f"Unknown engine: {name}. Available: {list(_ENGINES.keys())}"
        raise ValueError(msg)
    return cls()
