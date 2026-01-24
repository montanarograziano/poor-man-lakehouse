"""Dremio connection builder for Arrow Flight queries."""

from typing import ClassVar

import duckdb
import polars as pl
import pyarrow.dataset as ds
import requests
from loguru import logger
from pyarrow import flight
from pyarrow.flight import FlightClient

from poor_man_lakehouse.config import settings


class DremioConnection:
    """Connection manager for Dremio using Arrow Flight protocol.

    Handles authentication, catalog setup, and provides methods for
    querying data in various formats (Arrow, DuckDB, Polars, Pandas).
    """

    DEFAULT_REQUEST_TIMEOUT: ClassVar[int] = 30

    def __init__(self) -> None:
        """Initialize a Dremio connection with automatic setup."""
        self.apiv2_endpoint = f"{settings.DREMIO_SERVER_URI}/apiv2"
        self.apiv3_endpoint = f"{settings.DREMIO_SERVER_URI}/api/v3"
        self.sql_endpoint = f"{self.apiv3_endpoint}/sql"
        self.login_endpoint = self.apiv2_endpoint + "/login"
        self.bootstrap_endpoint = self.apiv2_endpoint + "/bootstrap/firstuser"

        self.catalog_name = settings.CATALOG_NAME
        self.source_endpoint = f"{settings.DREMIO_SERVER_URI}/api/v3/catalog"

        self.payload = {
            "userName": settings.DREMIO_USERNAME,
            "password": settings.DREMIO_ROOT_PASSWORD,
        }
        self.arrow_endpoint = f"{settings.ARROW_ENDPOINT}"

        self._initialize_dremio()

        self.token = self._get_token()
        self.headers = [(b"authorization", f"bearer {self.token}".encode("utf-8"))]
        self.client = FlightClient(location=(self.arrow_endpoint))
        logger.debug(f"Initialized DremioConnection with Arrow endpoint: {self.arrow_endpoint}")

    def query(self, query: str, client: FlightClient, headers: list) -> flight.FlightStreamReader:
        """Execute a query via Arrow Flight and return a stream reader.

        Args:
            query: SQL query string to execute.
            client: Arrow Flight client instance.
            headers: Authentication headers for the request.

        Returns:
            FlightStreamReader for reading query results.
        """
        options = flight.FlightCallOptions(headers=headers)
        flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
        return client.do_get(flight_info.endpoints[0].ticket, options)

    def to_arrow(self, query: str) -> flight.FlightStreamReader:
        """Execute query and return Arrow FlightStreamReader.

        Args:
            query: SQL query string to execute.

        Returns:
            FlightStreamReader for streaming Arrow data.
        """
        return self.query(query, self.client, self.headers)

    def to_duckdb(self, querystring: str):
        """Execute query and return a DuckDB relation.

        Args:
            querystring: SQL query string to execute.

        Returns:
            DuckDB relation containing query results.
        """
        stream_reader = self.query(querystring, self.client, self.headers)
        table = stream_reader.read_all()
        my_ds = ds.dataset(source=[table])
        return duckdb.arrow(my_ds)

    def to_polars(self, querystring: str) -> pl.DataFrame | pl.Series:
        """Execute query and return a Polars DataFrame or Series.

        Args:
            querystring: SQL query string to execute.

        Returns:
            Polars DataFrame or Series containing query results.
        """
        stream_reader = self.query(querystring, self.client, self.headers)
        table = stream_reader.read_all()
        return pl.from_arrow(table)

    def to_pandas(self, querystring: str):
        """Execute query and return a Pandas DataFrame.

        Args:
            querystring: SQL query string to execute.

        Returns:
            Pandas DataFrame containing query results.
        """
        stream_reader = self.query(querystring, self.client, self.headers)
        return stream_reader.read_pandas()

    def set_catalog(self, catalog_name: str) -> None:
        """Set the current catalog for Dremio queries.

        Args:
            catalog_name: Name of the catalog to set as current.
        """
        self.catalog_name = catalog_name
        self.to_polars(f"USE {self.catalog_name}")
        logger.debug(f"Catalog set to: {self.catalog_name}")

    def _get_token(self) -> str:
        """Retrieve PAT token from Dremio via login endpoint.

        Returns:
            Authentication token string.

        Raises:
            ValueError: If authentication fails or token is invalid.
        """
        response = requests.post(
            self.login_endpoint,
            json=self.payload,
            timeout=self.DEFAULT_REQUEST_TIMEOUT,
        )

        if response.status_code == 200:
            token: str = response.json().get("token", "")
            if not token:
                raise ValueError("Failed to retrieve a valid token from Dremio.")
            return token

        logger.error(f"Failed to get a valid response. Status code: {response.status_code}")
        raise ValueError(f"Failed to authenticate with Dremio. Status code: {response.status_code}")

    def _initialize_dremio(self) -> None:
        """Initialize Dremio by ensuring admin user and Nessie catalog exist."""
        logger.info("Initializing Dremio setup...")

        try:
            token = self._get_initial_token()
            if token:
                logger.info("Admin user authentication successful")
                self._ensure_nessie_catalog(token)
            else:
                logger.warning("Admin user authentication failed - attempting to create user")
                if self._create_admin_user():
                    logger.info("Admin user created successfully, retrying authentication")
                    token = self._get_initial_token()
                    if token:
                        logger.info("Admin user authentication successful after creation")
                        self._ensure_nessie_catalog(token)
                    else:
                        raise ValueError("Failed to authenticate even after creating admin user")
                else:
                    raise ValueError("Failed to create admin user")

        except Exception as e:
            logger.error(f"Failed to initialize Dremio: {e}")
            raise

    def _get_initial_token(self) -> str | None:
        """Get token for initial setup checks.

        Returns:
            Token string if successful, None otherwise.
        """
        try:
            response = requests.post(
                self.login_endpoint,
                json=self.payload,
                timeout=self.DEFAULT_REQUEST_TIMEOUT,
            )
            if response.status_code == 200:
                token = response.json().get("token", "")
                return token if token else None
            logger.warning(f"Initial token request failed. Status code: {response.status_code}")
            return None
        except requests.RequestException as e:
            logger.warning(f"Initial token request failed: {e}")
            return None

    def _create_admin_user(self) -> bool:
        """Create admin user if it doesn't exist.

        Returns:
            True if user was created successfully, False otherwise.
        """
        try:
            user_payload = {
                "userName": settings.DREMIO_USERNAME,
                "firstName": "firstName",
                "lastName": "lastName",
                "email": getattr(
                    settings,
                    "DREMIO_ROOT_EMAIL",
                    f"{settings.DREMIO_USERNAME}@example.com",
                ),
                "password": settings.DREMIO_ROOT_PASSWORD,
                "extra": None,
            }

            logger.info(f"Creating admin user: {user_payload['userName']} ({user_payload['email']})")

            response = requests.put(
                self.bootstrap_endpoint,
                json=user_payload,
                headers={"Content-Type": "application/json"},
                timeout=self.DEFAULT_REQUEST_TIMEOUT,
            )

            if response.status_code in [200, 201]:
                logger.info("Admin user created successfully")
                return True
            logger.error(f"Failed to create admin user. Status code: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False

        except requests.RequestException as e:
            logger.error(f"Request failed while creating admin user: {e}")
            return False

    def _ensure_nessie_catalog(self, token: str) -> None:
        """Ensure Nessie catalog exists, create if it doesn't.

        Args:
            token: Authentication token for API calls.

        Raises:
            ValueError: If catalog creation fails.
        """
        if self._check_nessie_catalog_exists(token):
            logger.info("Nessie catalog 'nessie' already exists")
        else:
            logger.info("Nessie catalog 'nessie' not found. Creating...")
            if self._create_nessie_catalog(token):
                logger.info("Nessie catalog created successfully!")
            else:
                logger.error("Failed to create Nessie catalog")
                raise ValueError("Failed to create Nessie catalog")

    def _check_nessie_catalog_exists(self, token: str) -> bool:
        """Check if Nessie catalog already exists.

        Args:
            token: Authentication token for API calls.

        Returns:
            True if catalog exists, False otherwise.
        """
        try:
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(
                f"{self.source_endpoint}/by-path/nessie",
                headers=headers,
                timeout=self.DEFAULT_REQUEST_TIMEOUT,
            )

            if response.status_code == 200:
                data: dict = response.json()
                result: bool = data.get("name") == "nessie"
                return result
            return False
        except requests.RequestException as e:
            logger.warning(f"Error checking Nessie catalog: {e}")
            return False

    def _create_nessie_catalog(self, token: str) -> bool:
        """Create Nessie catalog in Dremio.

        Args:
            token: Authentication token for API calls.

        Returns:
            True if catalog was created successfully, False otherwise.
        """
        payload = {
            "name": "nessie",
            "config": {
                "nessieEndpoint": "http://nessie:19120/api/v2",
                "nessieAuthType": "NONE",
                "storageProvider": "AWS",
                "awsRootPath": "warehouse",
                "credentialType": "ACCESS_KEY",
                "awsAccessKey": settings.AWS_ACCESS_KEY_ID,
                "awsAccessSecret": settings.AWS_SECRET_ACCESS_KEY,
                "azureAuthenticationType": "ACCESS_KEY",
                "googleAuthenticationType": "SERVICE_ACCOUNT_KEYS",
                "propertyList": [
                    {"name": "fs.s3a.path.style.access", "value": "true"},
                    {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                    {"name": "dremio.s3.compat", "value": "true"},
                ],
                "secure": False,
                "asyncEnabled": True,
                "isCachingEnabled": True,
                "maxCacheSpacePct": 100,
            },
            "accelerationRefreshPeriod": 3600000,
            "accelerationGracePeriod": 10800000,
            "accelerationActivePolicyType": "PERIOD",
            "accelerationRefreshSchedule": "0 0 8 * * *",
            "accelerationRefreshOnDataChanges": False,
            "type": "NESSIE",
            "accessControlList": {"userControls": [], "roleControls": []},
        }

        try:
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            response = requests.put(
                self.apiv2_endpoint + "/source/nessie",
                json=payload,
                headers=headers,
                timeout=self.DEFAULT_REQUEST_TIMEOUT,
            )

            if response.status_code in [200, 201]:
                return True
            logger.error(f"Failed to create Nessie catalog. Status code: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False

        except requests.RequestException as e:
            logger.error(f"Request failed while creating Nessie catalog: {e}")
            return False
