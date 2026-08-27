"""Streaming synthetic data generator for Delta Lake and DuckLake benchmarks.

Memory is bounded by chunk_size, independent of total_rows. The schema always carries
``event_date`` (``pa.date32``) as the canonical partition column, used by both engines.

Public API:
    - ``GeneratorSpec``: immutable configuration for one run.
    - ``StreamingGenerator(spec)``: single-use producer. Each instance must be consumed exactly
      once; instantiate a fresh one per benchmark run.
    - ``StreamingGenerator.iter_batches() -> Iterator[pa.RecordBatch]``: streamed primary data.
    - ``StreamingGenerator.arrow_reader() -> pa.RecordBatchReader``: zero-copy reader for
      ``write_deltalake`` and DuckDB ``INSERT INTO ... SELECT * FROM <reader>``.
    - ``StreamingGenerator.iter_merge_batches(overlap_ratio)``: merge source data with crafted IDs.
    - ``StreamingGenerator.merge_arrow_reader(overlap_ratio)``: merge as a record batch reader.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from .config import ColumnDef, SchemaConfig

PARTITION_COL = "event_date"
_CHARSET = np.frombuffer(b"abcdefghijklmnopqrstuvwxyz0123456789", dtype=np.uint8)
_STRUCT_FIELD_TYPES: dict[str, pa.DataType] = {
    "int32": pa.int32(),
    "varchar": pa.string(),
    "float64": pa.float64(),
}


# ─── Schema ────────────────────────────────────────────────────────────────


def _arrow_type(col: ColumnDef) -> pa.DataType:
    """Map a ColumnDef to its canonical Arrow type. Fails fast on unknown types."""
    if col.type == "int8":
        return pa.int8()
    if col.type == "int16":
        return pa.int16()
    if col.type == "int32":
        return pa.int32()
    if col.type == "int64":
        return pa.int64()
    if col.type == "float32":
        return pa.float32()
    if col.type == "float64":
        return pa.float64()
    if col.type == "decimal":
        return pa.decimal128(col.precision or 18, col.scale or 4)
    if col.type == "date":
        return pa.date32()
    if col.type == "datetime":
        return pa.timestamp("us")
    if col.type == "timestamp":
        return pa.timestamp("us", tz="UTC")
    if col.type == "varchar":
        # Dict-encoded; both Delta/Parquet and DuckLake honor this on write.
        return pa.dictionary(pa.int32(), pa.string())
    if col.type == "text":
        return pa.large_string()
    if col.type == "boolean":
        return pa.bool_()
    if col.type == "list":
        return pa.list_(pa.int32())
    if col.type == "map":
        return pa.map_(pa.string(), pa.int32())
    if col.type == "struct":
        fields = []
        for fdef in col.fields:
            fname, ftype = fdef.split(":")
            if ftype not in _STRUCT_FIELD_TYPES:
                msg = f"Unsupported struct field type: {ftype}"
                raise ValueError(msg)
            fields.append(pa.field(fname, _STRUCT_FIELD_TYPES[ftype]))
        return pa.struct(fields)
    msg = f"Unsupported column type: {col.type}"
    raise ValueError(msg)


def build_schema(cfg: SchemaConfig) -> pa.Schema:
    """Build the canonical Arrow schema: id_col, event_date, plus user columns."""
    fields = [
        pa.field(cfg.id_col, pa.int64(), nullable=False),
        pa.field(PARTITION_COL, pa.date32(), nullable=False),
    ]
    fields.extend(pa.field(c.name, _arrow_type(c)) for c in cfg.columns)
    return pa.schema(fields)


# ─── Column generators (vectorized) ────────────────────────────────────────


def _gen_text(n: int, avg: int, rng: np.random.Generator) -> pa.Array:
    """Build large_string from raw buffers; no Python list comprehension."""
    lengths = np.clip(rng.normal(avg, avg * 0.3, size=n).astype(np.int64), 1, avg * 3)
    total = int(lengths.sum())
    data = _CHARSET[rng.integers(0, len(_CHARSET), size=total)].tobytes()
    offsets = np.empty(n + 1, dtype=np.int64)
    offsets[0] = 0
    np.cumsum(lengths, out=offsets[1:])
    return pa.Array.from_buffers(
        pa.large_string(),
        n,
        [None, pa.py_buffer(offsets), pa.py_buffer(data)],
    )


def _gen_dict_string(n: int, pool: list[str], rng: np.random.Generator) -> pa.Array:
    """Dictionary-encoded string built without per-row Python."""
    idx = rng.integers(0, len(pool), size=n, dtype=np.int32)
    return pa.DictionaryArray.from_arrays(pa.array(idx), pa.array(pool, type=pa.string()))


def _gen_array(col: ColumnDef, n: int, rng: np.random.Generator) -> pa.Array:
    """Vectorized array generation per ColumnDef. Fails fast on unknown types."""
    t = col.type
    if t in {"int8", "int16", "int32", "int64"}:
        dtype = getattr(np, t)
        info = np.iinfo(dtype)
        return pa.array(rng.integers(info.min, info.max, size=n, dtype=dtype))
    if t == "float32":
        return pa.array(rng.uniform(-1e6, 1e6, size=n).astype(np.float32))
    if t == "float64":
        return pa.array(rng.uniform(-1e15, 1e15, size=n))
    if t == "decimal":
        precision = col.precision or 18
        scale = col.scale or 4
        bound = 10 ** (precision - scale)
        return pa.array(rng.uniform(-bound, bound, size=n).round(scale)).cast(pa.decimal128(precision, scale))
    if t == "date":
        days = rng.integers(0, 5 * 365, size=n, dtype=np.int32)
        return pa.array(days, type=pa.date32())
    if t == "datetime":
        us = rng.integers(1577836800_000_000, 1735689599_000_000, size=n, dtype=np.int64)
        return pa.array(us, type=pa.timestamp("us"))
    if t == "timestamp":
        us = rng.integers(1577836800_000_000, 1735689599_000_000, size=n, dtype=np.int64)
        return pa.array(us, type=pa.timestamp("us", tz="UTC"))
    if t == "boolean":
        return pa.array(rng.integers(0, 2, size=n, dtype=np.bool_))
    if t == "varchar":
        pool = [f"value_{i:03d}" for i in range(col.cardinality or 1000)]
        return _gen_dict_string(n, pool, rng)
    if t == "text":
        return _gen_text(n, col.avg_length or 128, rng)
    if t == "list":
        avg = col.avg_length or 5
        lengths = np.clip(rng.normal(avg, 2, size=n).astype(np.int32), 1, 20)
        offsets = np.empty(n + 1, dtype=np.int32)
        offsets[0] = 0
        np.cumsum(lengths, out=offsets[1:])
        flat = rng.integers(-(2**31), 2**31, size=int(lengths.sum()), dtype=np.int32)
        return pa.ListArray.from_arrays(pa.array(offsets), pa.array(flat))
    if t == "map":
        avg = col.avg_length or 3
        pool = [f"key_{i:02d}" for i in range(50)]
        lengths = np.clip(rng.normal(avg, 1, size=n).astype(np.int32), 1, len(pool))
        # DuckDB MAP requires unique keys per row, so sample without replacement per row.
        # This is a tight loop but bounded by avg_length (typically 3-5), so cost is low.
        all_keys: list[str] = []
        for n_keys in lengths:
            picks = rng.choice(len(pool), size=int(n_keys), replace=False)
            all_keys.extend(pool[i] for i in picks)
        total = len(all_keys)
        keys = pa.array(all_keys, type=pa.string())
        values = pa.array(rng.integers(-1000, 1000, size=total, dtype=np.int32))
        offsets = np.empty(n + 1, dtype=np.int32)
        offsets[0] = 0
        np.cumsum(lengths, out=offsets[1:])
        return pa.MapArray.from_arrays(pa.array(offsets), keys, values)
    if t == "struct":
        arrays: list[pa.Array] = []
        fields: list[pa.Field] = []
        for fdef in col.fields:
            fname, ftype = fdef.split(":")
            if ftype == "int32":
                arrays.append(pa.array(rng.integers(-(2**31), 2**31, size=n, dtype=np.int32)))
            elif ftype == "float64":
                arrays.append(pa.array(rng.uniform(-1e6, 1e6, size=n)))
            elif ftype == "varchar":
                pool = [f"s_{i:03d}" for i in range(100)]
                idx = rng.integers(0, len(pool), size=n, dtype=np.int32)
                arrays.append(pa.array([pool[i] for i in idx], type=pa.string()))
            else:
                msg = f"Unsupported struct field type: {ftype}"
                raise ValueError(msg)
            fields.append(pa.field(fname, _STRUCT_FIELD_TYPES[ftype]))
        return pa.StructArray.from_arrays(arrays, fields=fields)
    msg = f"Unsupported column type: {t}"
    raise ValueError(msg)


# ─── Generator ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class GeneratorSpec:
    """Immutable specification for one streaming-generation run.

    ``date_start``/``date_end`` define the cardinality of the ``event_date`` partition column.
    Default is a 30-day range so partition fan-out stays bounded at any row count: e.g. at 10K
    rows you get ~330 rows/partition, at 100M rows ~3.3M rows/partition, both reasonable for
    Parquet writers. A wider range (e.g. one year = 365 partitions) blows up DuckDB's writer
    memory at small scales because each open partition holds a multi-MB write buffer. Override
    only when you specifically want to stress partition cardinality.
    """

    schema_config: SchemaConfig
    total_rows: int
    chunk_size: int = 100_000
    seed: int = 42
    date_start: dt.date = dt.date(2024, 1, 1)
    date_end: dt.date = dt.date(2024, 1, 30)

    def __post_init__(self) -> None:
        """Validate inputs eagerly."""
        if self.total_rows <= 0:
            msg = "total_rows must be positive"
            raise ValueError(msg)
        if self.chunk_size <= 0:
            msg = "chunk_size must be positive"
            raise ValueError(msg)
        if self.date_end < self.date_start:
            msg = "date_end must be >= date_start"
            raise ValueError(msg)


class StreamingGenerator:
    """Single-use producer with multiple consumer adapters. Schema-stable across chunks.

    Contract: each ``StreamingGenerator`` instance produces output exactly once. To run another
    benchmark iteration, instantiate a fresh ``StreamingGenerator(spec)``.
    """

    def __init__(self, spec: GeneratorSpec) -> None:
        self.spec = spec
        self.schema = build_schema(spec.schema_config)
        self._date_span_days = (spec.date_end - spec.date_start).days + 1
        self._date_origin_days = (spec.date_start - dt.date(1970, 1, 1)).days

    # --- primary data ---

    def iter_batches(self) -> Iterator[pa.RecordBatch]:
        """Yield record batches sized by ``chunk_size``."""
        sub_seeds = np.random.SeedSequence(self.spec.seed).spawn(
            (self.spec.total_rows + self.spec.chunk_size - 1) // self.spec.chunk_size
        )
        rows_done = 0
        for sub_seed in sub_seeds:
            n = min(self.spec.chunk_size, self.spec.total_rows - rows_done)
            yield self._chunk(n, rows_done, np.random.default_rng(sub_seed))
            rows_done += n

    def arrow_reader(self) -> pa.RecordBatchReader:
        """Expose primary data as a single-pass ``pa.RecordBatchReader``."""
        return pa.RecordBatchReader.from_batches(self.schema, self.iter_batches())

    # --- merge data ---

    def iter_merge_batches(self, overlap_ratio: float) -> Iterator[pa.RecordBatch]:
        """Yield merge source batches: ``overlap_ratio`` reuses existing IDs, the rest are new.

        IDs are sorted ascending so the stream is partition-sorted (same partitioning function
        as the primary stream). This keeps the Parquet writer's open-buffer set bounded to
        1-2 partitions at any time. ``event_date`` is recomputed from each chunk's IDs so
        update rows land in the same partition as the base row.
        """
        if not 0.0 <= overlap_ratio <= 1.0:
            msg = "overlap_ratio must be in [0, 1]"
            raise ValueError(msg)
        n = self.spec.total_rows
        update_count = int(n * overlap_ratio)
        insert_count = n - update_count
        rng = np.random.default_rng(self.spec.seed + 1)
        update_ids = rng.choice(n, size=update_count, replace=False).astype(np.int64)
        insert_ids = np.arange(n, n + insert_count, dtype=np.int64)
        all_ids = np.concatenate([update_ids, insert_ids])
        all_ids.sort()  # partition-sorted stream: bounds writer fan-out to 1-2 open files.

        chunk_size = self.spec.chunk_size
        n_chunks = (len(all_ids) + chunk_size - 1) // chunk_size
        sub_seeds = np.random.SeedSequence(self.spec.seed + 2).spawn(n_chunks)
        cursor = 0
        id_field_idx = self.schema.get_field_index(self.spec.schema_config.id_col)
        date_field_idx = self.schema.get_field_index(PARTITION_COL)
        n_partitions = self._date_span_days
        rows_per_partition = max(1, (n + n_partitions - 1) // n_partitions)
        for sub_seed in sub_seeds:
            chunk_ids = all_ids[cursor : cursor + chunk_size]
            chunk = self._chunk(len(chunk_ids), 0, np.random.default_rng(sub_seed))
            # Override id with the merge IDs.
            chunk = chunk.set_column(
                id_field_idx,
                self.schema.field(id_field_idx),
                pa.array(chunk_ids, type=pa.int64()),
            )
            # Recompute event_date from the actual IDs (same formula as primary stream)
            # so update rows land in the same partition as the base row.
            partition_idx = np.minimum(chunk_ids // rows_per_partition, n_partitions - 1).astype(np.int32)
            date_offsets = self._date_origin_days + partition_idx
            chunk = chunk.set_column(
                date_field_idx,
                self.schema.field(date_field_idx),
                pa.array(date_offsets, type=pa.date32()),
            )
            yield chunk
            cursor += len(chunk_ids)

    def merge_arrow_reader(self, overlap_ratio: float) -> pa.RecordBatchReader:
        """Expose merge source as a single-pass ``pa.RecordBatchReader``."""
        return pa.RecordBatchReader.from_batches(self.schema, self.iter_merge_batches(overlap_ratio))

    # --- internals ---

    def _chunk(self, n: int, id_offset: int, rng: np.random.Generator) -> pa.RecordBatch:
        cfg = self.spec.schema_config
        ids_np = np.arange(id_offset, id_offset + n, dtype=np.int64)
        ids = pa.array(ids_np)

        # Assign event_date deterministically by ID position so rows are sorted by partition
        # in stream order. This bounds Parquet writer memory: only 1-2 partition writers are
        # ever open at once, regardless of partition count or total_rows. With random dates
        # the writer holds ALL partition buffers open in parallel and OOMs at scale.
        n_partitions = self._date_span_days
        rows_per_partition = max(1, (self.spec.total_rows + n_partitions - 1) // n_partitions)
        partition_idx = np.minimum(ids_np // rows_per_partition, n_partitions - 1).astype(np.int32)
        date_offsets = self._date_origin_days + partition_idx
        event_date = pa.array(date_offsets, type=pa.date32())

        arrays: list[pa.Array] = [ids, event_date]
        names: list[str] = [cfg.id_col, PARTITION_COL]
        for col in cfg.columns:
            arrays.append(_gen_array(col, n, rng))
            names.append(col.name)

        batch = pa.RecordBatch.from_arrays(arrays, names=names)
        # Cast enforces schema stability across chunks. Fails loudly on drift.
        return batch.cast(self.schema)
