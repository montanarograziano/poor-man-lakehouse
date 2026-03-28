"""Unit tests for the dremio_connector module."""

from unittest.mock import MagicMock, patch

import pytest


class TestDremioConnectionInit:
    """Tests for DremioConnection initialization."""

    @patch("poor_man_lakehouse.dremio_connector.builder.settings")
    @patch("poor_man_lakehouse.dremio_connector.builder.FlightClient")
    @patch("poor_man_lakehouse.dremio_connector.builder.requests")
    def test_init_builds_endpoints(self, mock_requests, mock_flight_client, mock_settings):
        """Test that __init__ constructs API endpoints correctly."""
        mock_settings.DREMIO_SERVER_URI = "http://dremio:9047"
        mock_settings.ARROW_ENDPOINT = "grpc://dremio:32010"
        mock_settings.DREMIO_USERNAME = "dremio"
        mock_settings.DREMIO_ROOT_PASSWORD = "password"
        mock_settings.DREMIO_ROOT_EMAIL = "admin@example.com"
        mock_settings.CATALOG_NAME = "nessie"
        mock_settings.AWS_ACCESS_KEY_ID = "minioadmin"
        mock_settings.AWS_SECRET_ACCESS_KEY = "miniopassword"

        # Mock successful auth flow
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"token": "test-token"}
        mock_requests.post.return_value = mock_response

        # Mock catalog check
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {"name": "nessie"}
        mock_requests.get.return_value = mock_get_response
        mock_requests.RequestException = Exception

        mock_flight_client.return_value = MagicMock()

        from poor_man_lakehouse.dremio_connector.builder import DremioConnection

        conn = DremioConnection()

        assert conn.apiv2_endpoint == "http://dremio:9047/apiv2"
        assert conn.apiv3_endpoint == "http://dremio:9047/api/v3"
        assert conn.sql_endpoint == "http://dremio:9047/api/v3/sql"
        assert conn.token == "test-token"

    @patch("poor_man_lakehouse.dremio_connector.builder.settings")
    @patch("poor_man_lakehouse.dremio_connector.builder.requests")
    def test_get_token_raises_on_auth_failure(self, mock_requests, mock_settings):
        """Test _get_token raises ValueError on authentication failure."""
        mock_settings.DREMIO_SERVER_URI = "http://dremio:9047"
        mock_settings.ARROW_ENDPOINT = "grpc://dremio:32010"
        mock_settings.DREMIO_USERNAME = "dremio"
        mock_settings.DREMIO_ROOT_PASSWORD = "wrong-password"
        mock_settings.DREMIO_ROOT_EMAIL = "admin@example.com"
        mock_settings.CATALOG_NAME = "nessie"
        mock_settings.AWS_ACCESS_KEY_ID = "minioadmin"
        mock_settings.AWS_SECRET_ACCESS_KEY = "miniopassword"
        mock_requests.RequestException = Exception

        # Mock failed auth for _get_initial_token
        mock_response_fail = MagicMock()
        mock_response_fail.status_code = 401
        mock_requests.post.return_value = mock_response_fail

        # Mock failed bootstrap
        mock_put_response = MagicMock()
        mock_put_response.status_code = 500
        mock_put_response.text = "Internal error"
        mock_requests.put.return_value = mock_put_response

        from poor_man_lakehouse.dremio_connector.builder import DremioConnection

        with pytest.raises(ValueError, match="Failed to create admin user"):
            DremioConnection()


class TestDremioConnectionDefaultTimeout:
    """Tests for default configuration values."""

    def test_default_request_timeout(self):
        """Test DEFAULT_REQUEST_TIMEOUT class variable."""
        from poor_man_lakehouse.dremio_connector.builder import DremioConnection

        assert DremioConnection.DEFAULT_REQUEST_TIMEOUT == 30


class TestDremioConnectionQuery:
    """Tests for query execution methods."""

    @patch("poor_man_lakehouse.dremio_connector.builder.settings")
    @patch("poor_man_lakehouse.dremio_connector.builder.FlightClient")
    @patch("poor_man_lakehouse.dremio_connector.builder.requests")
    def test_to_arrow_delegates_to_query(self, mock_requests, mock_flight_client, mock_settings):
        """Test to_arrow delegates to query method."""
        mock_settings.DREMIO_SERVER_URI = "http://dremio:9047"
        mock_settings.ARROW_ENDPOINT = "grpc://dremio:32010"
        mock_settings.DREMIO_USERNAME = "dremio"
        mock_settings.DREMIO_ROOT_PASSWORD = "password"
        mock_settings.DREMIO_ROOT_EMAIL = "admin@example.com"
        mock_settings.CATALOG_NAME = "nessie"
        mock_settings.AWS_ACCESS_KEY_ID = "minioadmin"
        mock_settings.AWS_SECRET_ACCESS_KEY = "miniopassword"
        mock_requests.RequestException = Exception

        # Mock successful auth
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"token": "test-token"}
        mock_requests.post.return_value = mock_response

        # Mock catalog exists
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {"name": "nessie"}
        mock_requests.get.return_value = mock_get_response

        mock_client_instance = MagicMock()
        mock_flight_client.return_value = mock_client_instance

        from poor_man_lakehouse.dremio_connector.builder import DremioConnection

        conn = DremioConnection()

        # Mock query execution
        mock_stream = MagicMock()
        mock_flight_info = MagicMock()
        mock_client_instance.get_flight_info.return_value = mock_flight_info
        mock_flight_info.endpoints = [MagicMock()]
        mock_client_instance.do_get.return_value = mock_stream

        result = conn.to_arrow("SELECT 1")
        assert result == mock_stream
