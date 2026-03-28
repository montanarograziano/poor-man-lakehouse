"""Fixtures for integration tests.

These tests require running Docker services. Start them with:
    docker compose --profile lakekeeper up -d

Skip integration tests in CI by default:
    pytest -m "not integration"
"""

import pytest

from poor_man_lakehouse.config import Settings


@pytest.fixture(scope="session")
def integration_settings():
    """Provide settings configured for integration tests.

    Expects services to be running via docker compose --profile lakekeeper.
    """
    return Settings(
        _env_file=None,
        CATALOG="lakekeeper",
        CATALOG_NAME="lakekeeper",
        AWS_ACCESS_KEY_ID="minioadmin",
        AWS_SECRET_ACCESS_KEY="miniopassword",  # noqa: S106
        AWS_ENDPOINT_URL="http://localhost:9000",
        AWS_DEFAULT_REGION="eu-central-1",
        LAKEKEEPER_SERVER_URI="http://localhost:8181/catalog",
        LAKEKEEPER_WAREHOUSE="warehouse",
        BUCKET_NAME="warehouse",
    )
