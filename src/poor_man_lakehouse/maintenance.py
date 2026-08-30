"""Iceberg table maintenance and health inspection.

Provides a read-only health/inspection API and dry-run-guarded maintenance
procedures for Iceberg tables managed by PyIceberg.

All mutating operations default to ``dry_run=True`` — they return a plan
describing what *would* happen without modifying the table. Pass
``dry_run=False`` explicitly to execute the operation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger
from pyiceberg.table import Table  # noqa: TC002


@dataclass(frozen=True)
class TableHealth:
    """Read-only health summary for an Iceberg table."""

    table_name: str
    format_version: int
    snapshot_count: int
    latest_snapshot_id: int | None
    latest_snapshot_timestamp_ms: int | None
    latest_snapshot_age_seconds: float | None
    current_schema_id: int
    partition_spec_id: int
    data_file_count: int
    delete_file_count: int
    total_records: int
    total_file_size_bytes: int
    metadata_log_entry_count: int
    table_properties: dict[str, str]


@dataclass(frozen=True)
class ExpireSnapshotsPlan:
    """Plan for snapshot expiration — returned in dry-run mode."""

    table_name: str
    snapshots_to_expire: list[dict[str, Any]]
    current_snapshot_id: int | None
    dry_run: bool
    older_than: datetime | None = None


@dataclass(frozen=True)
class MaintenanceResult:
    """Result of a maintenance operation."""

    operation: str
    table_name: str
    success: bool
    detail: str
    dry_run: bool
    affected_snapshot_ids: list[int] = field(default_factory=list)


class TableMaintenance:
    """Maintenance and health inspection for a single Iceberg table.

    Constructed from a PyIceberg Table instance (obtained via
    ``LakehouseConnection.load_table``). All mutating operations
    default to ``dry_run=True``.

    Example:
        >>> conn = LakehouseConnection()
        >>> m = TableMaintenance(
        ...     conn.load_table(
        ...         "default",
        ...         "events",
        ...     )
        ... )
        >>> health = (
        ...     m.table_health()
        ... )
        >>> plan = m.expire_snapshots(
        ...     older_than_days=7
        ... )  # dry run
    """

    def __init__(self, table: Table) -> None:
        """Initialize with a PyIceberg Table.

        Args:
            table: A loaded PyIceberg Table instance.
        """
        self._table = table

    @property
    def table(self) -> Table:
        """The underlying PyIceberg Table."""
        return self._table

    @property
    def table_name(self) -> str:
        """Table name as returned by PyIceberg.

        For catalog-loaded tables this is typically the last component
        (e.g. ``"events"``); for a fully qualified identifier pass-through
        it may be a dotted string.  Use ``LakehouseConnection.load_table``
        which already qualifies as ``namespace.table``.
        """
        name = self._table.name()
        if isinstance(name, tuple):
            return ".".join(name)
        return str(name)

    def table_health(self) -> TableHealth:
        """Compute a read-only health summary of the table.

        Returns:
            TableHealth dataclass with key metrics.
        """
        metadata = self._table.metadata
        snapshots = metadata.snapshots or []

        current_snap = self._table.current_snapshot()

        latest_snap_id: int | None = None
        latest_snap_ts: int | None = None
        latest_snap_age: float | None = None
        if current_snap is not None:
            latest_snap_id = current_snap.snapshot_id
            latest_snap_ts = current_snap.timestamp_ms
            latest_snap_age = datetime.now(tz=timezone.utc).timestamp() - current_snap.timestamp_ms / 1000

        # Count data/delete files and records from current snapshot summary
        data_file_count = 0
        delete_file_count = 0
        total_records = 0
        total_file_size_bytes = 0
        if current_snap and current_snap.summary:
            summary = current_snap.summary.model_dump()
            data_file_count = int(summary.get("total-data-files", 0))
            delete_file_count = int(summary.get("total-delete-files", 0))
            total_records = int(summary.get("total-records", 0))
            total_file_size_bytes = int(summary.get("total-files-size", 0))

        metadata_log_count = len(metadata.metadata_log) if metadata.metadata_log else 0

        return TableHealth(
            table_name=self.table_name,
            format_version=metadata.format_version,
            snapshot_count=len(snapshots),
            latest_snapshot_id=latest_snap_id,
            latest_snapshot_timestamp_ms=latest_snap_ts,
            latest_snapshot_age_seconds=latest_snap_age,
            current_schema_id=metadata.current_schema_id,
            partition_spec_id=metadata.default_spec_id,
            data_file_count=data_file_count,
            delete_file_count=delete_file_count,
            total_records=total_records,
            total_file_size_bytes=total_file_size_bytes,
            metadata_log_entry_count=metadata_log_count,
            table_properties=dict(metadata.properties),
        )

    def inspect_metadata(self) -> dict[str, Any]:
        """Return a rich metadata view of the table.

        Includes snapshot history, current schema, partition spec,
        sort order, table properties, and refs.

        Returns:
            Dictionary with structured metadata information.
        """
        metadata = self._table.metadata
        snapshots = metadata.snapshots or []

        snapshot_summary = [
            {
                "snapshot_id": s.snapshot_id,
                "timestamp_ms": s.timestamp_ms,
                "operation": s.summary.operation if s.summary else None,
                "manifest_list": s.manifest_list,
            }
            for s in snapshots
        ]

        schema_fields = [
            {
                "field_id": f.field_id,
                "name": f.name,
                "type": str(f.field_type),
                "required": f.required,
            }
            for f in self._table.schema().fields
        ]

        partition_fields = [
            {
                "field_id": pf.field_id,
                "source_id": pf.source_id,
                "name": pf.name,
                "transform": str(pf.transform),
            }
            for pf in self._table.spec().fields
        ]

        refs = {
            name: {"type": ref.snapshot_ref_type, "snapshot_id": ref.snapshot_id}
            for name, ref in (metadata.refs or {}).items()
        }

        return {
            "table_name": self.table_name,
            "format_version": metadata.format_version,
            "location": metadata.location,
            "current_schema": schema_fields,
            "partition_spec": partition_fields,
            "sort_order": [str(f) for f in self._table.sort_order().fields] if self._table.sort_order().fields else [],
            "table_properties": dict(metadata.properties),
            "snapshot_count": len(snapshots),
            "snapshot_history": snapshot_summary,
            "refs": refs,
            "metadata_log_entry_count": len(metadata.metadata_log) if metadata.metadata_log else 0,
        }

    def expire_snapshots(
        self,
        *,
        older_than_days: int | None = None,
        older_than: datetime | None = None,
        snapshot_ids: list[int] | None = None,
        dry_run: bool = True,
    ) -> ExpireSnapshotsPlan | MaintenanceResult:
        """Expire old snapshots to reclaim metadata space.

        At least one of ``older_than_days``, ``older_than``, or ``snapshot_ids``
        must be provided.

        Args:
            older_than_days: Expire snapshots older than this many days.
                Mutually exclusive with ``older_than``.
            older_than: Expire snapshots older than this datetime.
                Mutually exclusive with ``older_than_days``.
            snapshot_ids: Specific snapshot IDs to expire.
            dry_run: If True (default), return a plan without modifying the table.
                If False, execute the expiration.

        Returns:
            ExpireSnapshotsPlan in dry-run mode, MaintenanceResult otherwise.

        Raises:
            ValueError: If no expiration criteria provided, or if conflicting
                time-based arguments given.
        """
        # --- Input validation ---
        if older_than_days is not None and older_than is not None:
            raise ValueError("'older_than_days' and 'older_than' are mutually exclusive")
        if older_than_days is not None and older_than_days < 0:
            raise ValueError(f"'older_than_days' must be non-negative, got {older_than_days}")

        # Treat empty snapshot_ids as "no criteria" (Bug #4)
        effective_ids: set[int] | None = None
        if snapshot_ids is not None:
            effective_ids = set(snapshot_ids) if snapshot_ids else None

        has_criteria = older_than_days is not None or older_than is not None or effective_ids is not None
        if not has_criteria:
            raise ValueError("At least one of 'older_than_days', 'older_than', or 'snapshot_ids' must be provided")

        # --- Compute cutoff datetime ---
        cutoff: datetime | None = None
        if older_than_days is not None:
            from datetime import timedelta

            cutoff = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            cutoff = cutoff - timedelta(days=older_than_days)
        elif older_than is not None:
            # Normalize naive datetimes to UTC (Bug #3)
            cutoff = older_than if older_than.tzinfo is not None else older_than.replace(tzinfo=timezone.utc)

        # --- Gather protected snapshot IDs ---
        metadata = self._table.metadata
        all_snapshots = metadata.snapshots or []
        current_snap = self._table.current_snapshot()
        current_id = current_snap.snapshot_id if current_snap else None

        # Snapshots referenced by branches/tags are protected (Bug #1)
        protected_ids: set[int] = set()
        if current_id is not None:
            protected_ids.add(current_id)
        for ref in (metadata.refs or {}).values():
            protected_ids.add(ref.snapshot_id)

        # --- Select candidates ---
        candidates: list[dict[str, Any]] = []
        candidate_ids: list[int] = []

        for snap in all_snapshots:
            if snap.snapshot_id in protected_ids:
                continue

            should_expire = False
            if effective_ids is not None and snap.snapshot_id in effective_ids:
                should_expire = True
            elif cutoff is not None:
                snap_dt = datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=timezone.utc)
                if snap_dt < cutoff:
                    should_expire = True

            if should_expire:
                candidates.append(
                    {
                        "snapshot_id": snap.snapshot_id,
                        "timestamp_ms": snap.timestamp_ms,
                        "operation": snap.summary.operation if snap.summary else None,
                    }
                )
                candidate_ids.append(snap.snapshot_id)

        if dry_run:
            logger.info(
                f"[DRY RUN] expire_snapshots: {len(candidates)} snapshot(s) would be expired from '{self.table_name}'"
            )
            return ExpireSnapshotsPlan(
                table_name=self.table_name,
                snapshots_to_expire=candidates,
                current_snapshot_id=current_id,
                dry_run=True,
                older_than=cutoff,
            )

        # Execute expiration
        if not candidate_ids:
            return MaintenanceResult(
                operation="expire_snapshots",
                table_name=self.table_name,
                success=True,
                detail="No snapshots matched the expiration criteria",
                dry_run=False,
            )

        expire_builder = self._table.maintenance.expire_snapshots()
        expire_builder = expire_builder.by_ids(candidate_ids)
        expire_builder.commit()

        logger.info(f"Expired {len(candidate_ids)} snapshot(s) from '{self.table_name}'")
        return MaintenanceResult(
            operation="expire_snapshots",
            table_name=self.table_name,
            success=True,
            detail=f"Expired {len(candidate_ids)} snapshot(s)",
            dry_run=False,
            affected_snapshot_ids=candidate_ids,
        )

    def rewrite_manifests(self, *, dry_run: bool = True) -> MaintenanceResult:
        """Rewrite manifest files to optimize metadata.

        .. note::
            PyIceberg does not currently support manifest rewriting.
            Use Spark's ``rewriteManifests`` procedure for this operation.

        Args:
            dry_run: Ignored — always raises NotImplementedError.

        Raises:
            NotImplementedError: Always. Manifest rewriting requires Spark.
        """
        raise NotImplementedError(
            "Manifest rewriting is not supported by PyIceberg. "
            "Use Spark: CALL catalog.system.rewrite_manifests('namespace.table')"
        )

    def remove_orphan_files(self, *, dry_run: bool = True) -> MaintenanceResult:
        """Remove orphan files not referenced by any snapshot.

        .. note::
            PyIceberg does not currently support orphan file removal.
            Use Spark's ``removeOrphanFiles`` procedure for this operation.

        Args:
            dry_run: Ignored — always raises NotImplementedError.

        Raises:
            NotImplementedError: Always. Orphan file removal requires Spark.
        """
        raise NotImplementedError(
            "Orphan file removal is not supported by PyIceberg. "
            "Use Spark: CALL catalog.system.remove_orphan_files(table => 'namespace.table')"
        )

    def compute_statistics(self, *, dry_run: bool = True) -> MaintenanceResult:
        """Compute or update table statistics.

        .. note::
            PyIceberg does not currently support computing statistics.
            Use Spark's ``computeTableStats`` procedure for this operation.

        Args:
            dry_run: Ignored — always raises NotImplementedError.

        Raises:
            NotImplementedError: Always. Statistics computation requires Spark.
        """
        raise NotImplementedError(
            "Statistics computation is not supported by PyIceberg. "
            "Use Spark: CALL catalog.system.compute_table_stats('namespace.table')"
        )
