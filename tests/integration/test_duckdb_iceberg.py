"""Integration test: DuckDB-native Iceberg operations via Lakekeeper.

Exercises the duckdb-iceberg capabilities added between DuckDB 1.4 and 1.5.4:
partitioned CREATE TABLE, INSERT, merge-on-read UPDATE/DELETE, MERGE INTO,
time travel (AT VERSION), native metadata functions, CTAS, and DROP TABLE.

Requires Docker (services are spun up via testcontainers, see conftest).
Note: reading vended-credential tables from the host requires the README's
/etc/hosts entry `127.0.0.1 minio` because Lakekeeper vends the docker-internal
MinIO endpoint.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

NAMESPACE = "duckdb_iceberg_it"


def _rows(expr) -> list[dict]:
    """Materialize an ibis expression as a list of row dicts sorted by id when present."""
    rows = expr.to_pyarrow().to_pylist()
    if rows and "id" in rows[0]:
        rows.sort(key=lambda r: r["id"])
    return rows


@pytest.mark.integration
def test_duckdb_iceberg_full_roundtrip(integration_settings):
    """Staged roundtrip covering DDL, DML, time travel, and metadata inspection."""
    from poor_man_lakehouse.lakehouse import LakehouseConnection

    catalog_name = integration_settings.CATALOG_NAME
    fqn = f"{catalog_name}.{NAMESPACE}.events"

    with LakehouseConnection(catalog_type="lakekeeper") as conn:
        # -- Setup (idempotent: MinIO/Postgres data persists across sessions) --
        if NAMESPACE not in conn.list_namespaces():
            conn.create_namespace(NAMESPACE)
        conn.drop_table(NAMESPACE, "events")
        conn.drop_table(NAMESPACE, "events_copy")

        # -- Partitioned create + insert --
        conn.create_table(
            NAMESPACE,
            "events",
            "id BIGINT, name VARCHAR, event_time TIMESTAMP",
            partition_by="day(event_time)",
        )
        conn.write_table(
            NAMESPACE,
            "events",
            query=(
                "SELECT 1::BIGINT AS id, 'alpha' AS name, TIMESTAMP '2026-01-01 10:00:00' AS event_time "
                "UNION ALL SELECT 2, 'beta', TIMESTAMP '2026-01-02 11:00:00'"
            ),
        )

        # -- Snapshot bookkeeping for time travel --
        snapshots_before = _rows(conn.inspect_table(NAMESPACE, "events", aspect="snapshots"))
        assert snapshots_before
        latest = max(snapshots_before, key=lambda s: s["sequence_number"])
        first_snapshot_id = int(latest["snapshot_id"])

        # -- Merge-on-read DML through execute() --
        conn.execute(f"UPDATE {fqn} SET name = 'alpha-v2' WHERE id = 1")  # noqa: S608
        conn.execute(f"DELETE FROM {fqn} WHERE id = 2")  # noqa: S608
        conn.execute(
            f"MERGE INTO {fqn} AS t "  # noqa: S608
            "USING (SELECT 3::BIGINT AS id, 'gamma' AS name, TIMESTAMP '2026-01-03 09:00:00' AS event_time) AS s "
            "ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET name = s.name "
            "WHEN NOT MATCHED THEN INSERT (id, name, event_time) VALUES (s.id, s.name, s.event_time)"
        )

        current = _rows(conn.scan_duckdb(NAMESPACE, "events"))
        assert [r["id"] for r in current] == [1, 3]
        assert [r["name"] for r in current] == ["alpha-v2", "gamma"]

        # -- Time travel back to the first insert --
        historical = _rows(conn.scan_duckdb(NAMESPACE, "events", snapshot_id=first_snapshot_id))
        assert [r["id"] for r in historical] == [1, 2]
        assert [r["name"] for r in historical] == ["alpha", "beta"]

        # -- Native metadata functions --
        assert _rows(conn.inspect_table(NAMESPACE, "events", aspect="manifests"))
        assert _rows(conn.inspect_table(NAMESPACE, "events", aspect="column_stats"))

        # -- CTAS + drop --
        conn.create_table_as(NAMESPACE, "events_copy", query=f"SELECT * FROM {fqn}")  # noqa: S608
        assert "events_copy" in conn.list_tables(NAMESPACE)
        assert len(_rows(conn.scan_duckdb(NAMESPACE, "events_copy"))) == 2

        conn.drop_table(NAMESPACE, "events_copy")
        conn.drop_table(NAMESPACE, "events")
        remaining = conn.list_tables(NAMESPACE)
        assert "events" not in remaining
        assert "events_copy" not in remaining
