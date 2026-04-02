"""Fixtures for integration tests.

Services are spun up automatically via testcontainers (Docker Compose).
No manual `just up` required — just run:
    just test-integration
"""

import time
from pathlib import Path
from unittest.mock import patch

import pytest
import requests
from testcontainers.compose import DockerCompose

from poor_man_lakehouse.config import Settings

PROJECT_ROOT = Path(__file__).parent.parent.parent
LAKEKEEPER_READY_TIMEOUT = 60  # seconds


def _wait_for_lakekeeper(port: int) -> None:
    """Poll Lakekeeper's health endpoint until it responds."""
    url = f"http://localhost:{port}/health"
    deadline = time.monotonic() + LAKEKEEPER_READY_TIMEOUT
    while time.monotonic() < deadline:
        try:
            resp = requests.get(url, timeout=2)
            if resp.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        time.sleep(1)
    raise TimeoutError(f"Lakekeeper did not become ready on port {port} within {LAKEKEEPER_READY_TIMEOUT}s")


@pytest.fixture(scope="session")
def lakekeeper_compose():
    """Start the full Lakekeeper stack via Docker Compose and tear it down after the session."""
    with DockerCompose(
        context=PROJECT_ROOT,
        profiles=["lakekeeper"],
        build=True,
        wait=False,
    ) as compose:
        port = compose.get_service_port("lakekeeper", 8181)
        if port is None:
            raise RuntimeError("Lakekeeper port 8181 is not mapped")
        _wait_for_lakekeeper(port)
        yield compose


@pytest.fixture(scope="session")
def integration_settings(lakekeeper_compose):
    """Provide settings pointing to the testcontainers-managed Lakekeeper stack.

    Patches the global settings singleton in all modules that use it.
    """
    lakekeeper_port = lakekeeper_compose.get_service_port("lakekeeper", 8181)
    minio_port = lakekeeper_compose.get_service_port("minio", 9000)
    if lakekeeper_port is None or minio_port is None:
        raise RuntimeError("Required service ports are not mapped")

    s = Settings(
        CATALOG="lakekeeper",
        CATALOG_NAME="lakekeeper",
        AWS_ACCESS_KEY_ID="minioadmin",
        AWS_SECRET_ACCESS_KEY="miniopassword",  # noqa: S106
        AWS_ENDPOINT_URL=f"http://localhost:{minio_port}",
        AWS_DEFAULT_REGION="eu-central-1",
        LAKEKEEPER_SERVER_URI=f"http://localhost:{lakekeeper_port}/catalog",
        LAKEKEEPER_WAREHOUSE="warehouse",
        BUCKET_NAME="warehouse",
    )
    with (
        patch("poor_man_lakehouse.catalog.settings", s),
        patch("poor_man_lakehouse.lakehouse.settings", s),
    ):
        yield s
