"""Integration test: catalog operations via Lakekeeper.

Requires: docker compose --profile lakekeeper up -d
Run with: uv run pytest tests/integration/ -m integration -v
"""

import pytest


@pytest.mark.integration
def test_get_catalog_list_namespaces(integration_settings):
    """Test that get_catalog can list namespaces from a running Lakekeeper."""
    from poor_man_lakehouse.catalog import get_catalog

    catalog = get_catalog(catalog_type="lakekeeper")
    namespaces = catalog.list_namespaces()
    assert isinstance(namespaces, list)


@pytest.mark.integration
def test_lakehouse_connection_list_namespaces(integration_settings):
    """Test that LakehouseConnection can list namespaces from a running Lakekeeper."""
    from poor_man_lakehouse.lakehouse import LakehouseConnection

    with LakehouseConnection(catalog_type="lakekeeper") as conn:
        namespaces = conn.list_namespaces()
        assert isinstance(namespaces, list)
