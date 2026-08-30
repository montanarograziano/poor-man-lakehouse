"""Unit tests for the maintenance module."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from poor_man_lakehouse.maintenance import (
    ExpireSnapshotsPlan,
    MaintenanceResult,
    TableHealth,
    TableMaintenance,
)

pytestmark = pytest.mark.unit


def _make_mock_table(
    *,
    snapshots: list | None = None,
    current_snapshot: MagicMock | None = None,
    format_version: int = 2,
    properties: dict | None = None,
    metadata_log: list | None = None,
    schema_fields: list | None = None,
    spec_fields: list | None = None,
    sort_order_fields: list | None = None,
    refs: dict | None = None,
    location: str = "s3://warehouse/default/test_table",
) -> MagicMock:
    """Create a mock PyIceberg Table with realistic metadata."""
    table = MagicMock()

    # Table name
    table.name.return_value = ("default", "test_table")

    # Metadata
    metadata = MagicMock()
    metadata.format_version = format_version
    metadata.snapshots = snapshots or []
    metadata.current_schema_id = 0
    metadata.default_spec_id = 0
    metadata.properties = properties or {}
    metadata.metadata_log = metadata_log or []
    metadata.location = location
    metadata.refs = refs or {}
    table.metadata = metadata

    # Current snapshot
    table.current_snapshot.return_value = current_snapshot

    # Schema
    if schema_fields is None:
        mock_field = MagicMock()
        mock_field.field_id = 1
        mock_field.name = "id"
        mock_field.field_type = "long"
        mock_field.required = True
        schema_fields = [mock_field]
    table.schema.return_value.fields = schema_fields

    # Partition spec
    table.spec.return_value.fields = spec_fields or []

    # Sort order
    table.sort_order.return_value.fields = sort_order_fields or []

    return table


def _make_snapshot(
    snapshot_id: int,
    timestamp_ms: int,
    operation: str = "append",
    manifest_list: str = "s3://warehouse/metadata/snap.avro",
    summary_extras: dict | None = None,
) -> MagicMock:
    """Create a mock Iceberg snapshot."""
    snap = MagicMock()
    snap.snapshot_id = snapshot_id
    snap.timestamp_ms = timestamp_ms
    snap.manifest_list = manifest_list
    summary = MagicMock()
    summary.operation = operation
    summary_dict = {
        "operation": operation,
        "total-data-files": "10",
        "total-delete-files": "0",
        "total-records": "1000",
        "total-files-size": "5242880",
    }
    if summary_extras:
        summary_dict.update(summary_extras)
    summary.model_dump.return_value = summary_dict
    snap.summary = summary
    return snap


class TestTableMaintenance:
    """Tests for TableMaintenance construction."""

    def test_table_name_from_tuple(self):
        """Test table_name extracts last element from tuple."""
        table = _make_mock_table()
        m = TableMaintenance(table)
        assert m.table_name == "test_table"

    def test_table_name_from_string(self):
        """Test table_name handles string name."""
        table = _make_mock_table()
        table.name.return_value = "my_table"
        m = TableMaintenance(table)
        assert m.table_name == "my_table"

    def test_table_property(self):
        """Test table property returns the underlying table."""
        table = _make_mock_table()
        m = TableMaintenance(table)
        assert m.table is table


class TestTableHealth:
    """Tests for table_health()."""

    def test_health_returns_expected_fields(self):
        """Test table_health returns a TableHealth with all expected fields."""
        current_snap = _make_snapshot(
            snapshot_id=100,
            timestamp_ms=int(datetime(2026, 8, 1, tzinfo=timezone.utc).timestamp() * 1000),
        )
        table = _make_mock_table(
            snapshots=[current_snap],
            current_snapshot=current_snap,
            format_version=2,
            properties={"write.metadata.delete-after-commit.enabled": "true"},
            metadata_log=[MagicMock(), MagicMock()],
        )

        m = TableMaintenance(table)
        health = m.table_health()

        assert isinstance(health, TableHealth)
        assert health.format_version == 2
        assert health.snapshot_count == 1
        assert health.latest_snapshot_id == 100
        assert health.latest_snapshot_timestamp_ms is not None
        assert health.latest_snapshot_age_seconds is not None
        assert health.latest_snapshot_age_seconds > 0
        assert health.data_file_count == 10
        assert health.delete_file_count == 0
        assert health.total_records == 1000
        assert health.total_file_size_bytes == 5242880
        assert health.metadata_log_entry_count == 2
        assert "write.metadata.delete-after-commit.enabled" in health.table_properties

    def test_health_no_snapshots(self):
        """Test table_health handles tables with no snapshots."""
        table = _make_mock_table(snapshots=[], current_snapshot=None)
        m = TableMaintenance(table)
        health = m.table_health()

        assert health.snapshot_count == 0
        assert health.latest_snapshot_id is None
        assert health.latest_snapshot_age_seconds is None
        assert health.data_file_count == 0
        assert health.total_records == 0


class TestInspectMetadata:
    """Tests for inspect_metadata()."""

    def test_inspect_returns_structured_metadata(self):
        """Test inspect_metadata returns a dict with expected keys."""
        snap = _make_snapshot(snapshot_id=1, timestamp_ms=1000000)
        table = _make_mock_table(
            snapshots=[snap],
            current_snapshot=snap,
            properties={"key": "value"},
        )

        m = TableMaintenance(table)
        result = m.inspect_metadata()

        assert result["format_version"] == 2
        assert result["table_name"] == "test_table"
        assert len(result["snapshot_history"]) == 1
        assert result["snapshot_history"][0]["snapshot_id"] == 1
        assert len(result["current_schema"]) == 1
        assert result["current_schema"][0]["name"] == "id"
        assert result["table_properties"] == {"key": "value"}
        assert "partition_spec" in result
        assert "sort_order" in result
        assert "refs" in result


class TestExpireSnapshots:
    """Tests for expire_snapshots()."""

    def test_dry_run_returns_plan(self):
        """Test expire_snapshots dry_run=True returns an ExpireSnapshotsPlan."""
        old_ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        current_ts = int(datetime(2026, 8, 30, tzinfo=timezone.utc).timestamp() * 1000)

        old_snap = _make_snapshot(snapshot_id=1, timestamp_ms=old_ts)
        current_snap = _make_snapshot(snapshot_id=2, timestamp_ms=current_ts)

        table = _make_mock_table(
            snapshots=[old_snap, current_snap],
            current_snapshot=current_snap,
        )

        m = TableMaintenance(table)
        result = m.expire_snapshots(older_than_days=30, dry_run=True)

        assert isinstance(result, ExpireSnapshotsPlan)
        assert result.dry_run is True
        assert len(result.snapshots_to_expire) == 1
        assert result.snapshots_to_expire[0]["snapshot_id"] == 1
        assert result.current_snapshot_id == 2

    def test_dry_run_never_expires_current(self):
        """Test that the current snapshot is never included in expiration plan."""
        old_ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        snap = _make_snapshot(snapshot_id=1, timestamp_ms=old_ts)
        table = _make_mock_table(snapshots=[snap], current_snapshot=snap)

        m = TableMaintenance(table)
        result = m.expire_snapshots(older_than_days=1, dry_run=True)

        assert isinstance(result, ExpireSnapshotsPlan)
        assert len(result.snapshots_to_expire) == 0

    def test_dry_run_does_not_call_commit(self):
        """Test that dry_run=True never calls maintenance APIs."""
        old_ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        old_snap = _make_snapshot(snapshot_id=1, timestamp_ms=old_ts)
        current_snap = _make_snapshot(snapshot_id=2, timestamp_ms=int(datetime.now(tz=timezone.utc).timestamp() * 1000))
        table = _make_mock_table(snapshots=[old_snap, current_snap], current_snapshot=current_snap)

        m = TableMaintenance(table)
        m.expire_snapshots(older_than_days=1, dry_run=True)

        table.maintenance.assert_not_called()

    def test_raises_without_criteria(self):
        """Test expire_snapshots raises when no criteria provided."""
        table = _make_mock_table()
        m = TableMaintenance(table)
        with pytest.raises(ValueError, match="At least one of"):
            m.expire_snapshots(dry_run=True)

    def test_raises_conflicting_time_args(self):
        """Test expire_snapshots raises with both older_than and older_than_days."""
        table = _make_mock_table()
        m = TableMaintenance(table)
        with pytest.raises(ValueError, match="mutually exclusive"):
            m.expire_snapshots(
                older_than_days=7,
                older_than=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )

    def test_execute_calls_pyiceberg(self):
        """Test expire_snapshots dry_run=False calls PyIceberg maintenance API."""
        old_ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        old_snap = _make_snapshot(snapshot_id=1, timestamp_ms=old_ts)
        current_snap = _make_snapshot(snapshot_id=2, timestamp_ms=int(datetime.now(tz=timezone.utc).timestamp() * 1000))
        table = _make_mock_table(snapshots=[old_snap, current_snap], current_snapshot=current_snap)

        # Mock the maintenance chain (maintenance is a property, not a method)
        mock_expire = MagicMock()
        mock_expire.by_ids.return_value = mock_expire
        table.maintenance.expire_snapshots.return_value = mock_expire

        m = TableMaintenance(table)
        result = m.expire_snapshots(older_than_days=1, dry_run=False)

        assert isinstance(result, MaintenanceResult)
        assert result.success is True
        assert result.dry_run is False
        assert 1 in result.affected_snapshot_ids
        mock_expire.by_ids.assert_called_once_with([1])
        mock_expire.commit.assert_called_once()

    def test_expire_by_snapshot_ids(self):
        """Test expire_snapshots with explicit snapshot IDs."""
        snap1 = _make_snapshot(snapshot_id=10, timestamp_ms=1000)
        snap2 = _make_snapshot(snapshot_id=20, timestamp_ms=2000)
        current = _make_snapshot(snapshot_id=30, timestamp_ms=3000)
        table = _make_mock_table(snapshots=[snap1, snap2, current], current_snapshot=current)

        m = TableMaintenance(table)
        result = m.expire_snapshots(snapshot_ids=[10, 20], dry_run=True)

        assert isinstance(result, ExpireSnapshotsPlan)
        assert len(result.snapshots_to_expire) == 2
        ids = [s["snapshot_id"] for s in result.snapshots_to_expire]
        assert 10 in ids
        assert 20 in ids


class TestUnsupportedOperations:
    """Tests for operations not supported by PyIceberg."""

    def test_rewrite_manifests_raises(self):
        """Test rewrite_manifests raises NotImplementedError."""
        table = _make_mock_table()
        m = TableMaintenance(table)
        with pytest.raises(NotImplementedError, match="Manifest rewriting"):
            m.rewrite_manifests()

    def test_remove_orphan_files_raises(self):
        """Test remove_orphan_files raises NotImplementedError."""
        table = _make_mock_table()
        m = TableMaintenance(table)
        with pytest.raises(NotImplementedError, match="Orphan file removal"):
            m.remove_orphan_files()

    def test_compute_statistics_raises(self):
        """Test compute_statistics raises NotImplementedError."""
        table = _make_mock_table()
        m = TableMaintenance(table)
        with pytest.raises(NotImplementedError, match="Statistics computation"):
            m.compute_statistics()


class TestLakehouseConnectionMaintenance:
    """Tests for LakehouseConnection.maintenance() integration."""

    @patch("poor_man_lakehouse.lakehouse.get_catalog")
    def test_maintenance_returns_table_maintenance(self, mock_get_catalog):
        """Test LakehouseConnection.maintenance() returns TableMaintenance."""
        mock_table = _make_mock_table()
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_get_catalog.return_value = mock_catalog

        from poor_man_lakehouse.lakehouse import LakehouseConnection

        conn = LakehouseConnection()
        m = conn.maintenance("default", "test_table")

        assert isinstance(m, TableMaintenance)
        assert m.table is mock_table
        mock_catalog.load_table.assert_called_once_with("default.test_table")
