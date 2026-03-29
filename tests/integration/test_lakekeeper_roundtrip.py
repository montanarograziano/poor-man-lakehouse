"""Integration test: catalog browsing via Lakekeeper.

Requires: docker compose --profile lakekeeper up -d
Run with: uv run pytest tests/integration/ -m integration -v
"""

import pytest


@pytest.mark.integration
def test_pyiceberg_list_namespaces(integration_settings):
    """Test that PyIcebergClient can list namespaces from a running Lakekeeper."""
    from poor_man_lakehouse.pyiceberg_connector.client import PyIcebergClient

    client = PyIcebergClient(
        catalog_uri=integration_settings.LAKEKEEPER_SERVER_URI,
        catalog_name=integration_settings.CATALOG_NAME,
        storage_options=integration_settings.ICEBERG_STORAGE_OPTIONS,
    )
    namespaces = client.list_namespaces()
    assert isinstance(namespaces, list)


@pytest.mark.integration
def test_catalog_browser_list_namespaces(integration_settings):
    """Test that CatalogBrowser can list namespaces from a running Lakekeeper."""
    from poor_man_lakehouse.catalog_browser import CatalogBrowser

    browser = CatalogBrowser(
        catalog_uri=integration_settings.LAKEKEEPER_SERVER_URI,
        catalog_name=integration_settings.CATALOG_NAME,
    )
    namespaces = browser.list_namespaces()
    assert isinstance(namespaces, list)
