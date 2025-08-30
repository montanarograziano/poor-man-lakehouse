import duckdb
import polars as pl
import pyarrow.dataset as ds
import requests
from loguru import logger
from pyarrow import flight
from pyarrow.flight import FlightClient

from poor_man_lakehouse.config import settings


class DremioConnection:
    def __init__(self):
        ## URL to Login Endpoint
        self.apiv2_endpoint = f"{settings.DREMIO_SERVER_URI}/apiv2"
        self.apiv3_endpoint = f"{settings.DREMIO_SERVER_URI}/api/v3"
        self.sql_endpoint = f"{self.apiv3_endpoint}/sql"
        self.login_endpoint = self.apiv2_endpoint + "/login"
        self.bootstrap_endpoint = self.apiv2_endpoint + "/bootstrap/firstuser"

        self.catalog_name = settings.CATALOG_NAME
        self.source_endpoint = f"{settings.DREMIO_SERVER_URI}/api/v3/catalog"

        ## Payload for Login
        self.payload = {
            "userName": settings.DREMIO_USERNAME,
            "password": settings.DREMIO_ROOT_PASSWORD,
        }
        self.arrow_endpoint = f"{settings.ARROW_ENDPOINT}"

        # Initialize Dremio setup (user and catalog)
        self._initialize_dremio()

        # Get token and setup client
        self.token = self._get_token()
        self.headers = [(b"authorization", f"bearer {self.token}".encode("utf-8"))]
        self.client = FlightClient(location=(self.arrow_endpoint))
        logger.debug(
            f"Initialized DremioConnection with Arrow endpoint: {self.arrow_endpoint}"
        )

    def query(self, query, client, headers):
        ## Options for Query
        options = flight.FlightCallOptions(headers=headers)

        flight_info = client.get_flight_info(
            flight.FlightDescriptor.for_command(query), options
        )

        results = client.do_get(flight_info.endpoints[0].ticket, options)
        return results

    # Returns a FlightStreamReader
    def to_arrow(self, query):
        return self.query(query, self.client, self.headers)

    # Returns a DuckDB Relation
    def to_duckdb(self, querystring):
        streamReader = self.query(querystring, self.client, self.headers)
        table = streamReader.read_all()
        my_ds = ds.dataset(source=[table])
        return duckdb.arrow(my_ds)

    # Returns a Polars Dataframe
    def to_polars(self, querystring):
        streamReader = self.query(querystring, self.client, self.headers)
        table = streamReader.read_all()
        df = pl.from_arrow(table)
        return df

    # Returns a Pandas Dataframe
    def to_pandas(self, querystring):
        streamReader = self.query(querystring, self.client, self.headers)
        df = streamReader.read_pandas()
        return df

    def set_catalog(self, catalog_name: str):
        """
        Set the catalog for Dremio queries.
        :param catalog_name: Name of the catalog to set.
        """
        self.catalog_name = catalog_name
        self.to_polars(f"USE {self.catalog_name}")
        logger.debug(f"Catalog set to: {self.catalog_name}")

    ## Function to Retrieve PAT Token from Dremio
    def _get_token(self):
        # Make the POST request
        response = requests.post(self.login_endpoint, json=self.payload)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            token = response.json().get("token", "")
            if token is None or token == "":
                raise ValueError("Failed to retrieve a valid token from Dremio.")

            return token
        else:
            logger.error(
                f"Failed to get a valid response. Status code: {response.status_code}"
            )
            raise ValueError(
                f"Failed to authenticate with Dremio. Status code: {response.status_code}"
            )

    def _initialize_dremio(self):
        """Initialize Dremio by ensuring admin user and Nessie catalog exist."""
        logger.info("Initializing Dremio setup...")

        # Try to get token to check if admin user exists and is valid
        try:
            token = self._get_initial_token()
            if token:
                logger.info("Admin user authentication successful")
                # Check and create Nessie catalog if needed
                self._ensure_nessie_catalog(token)
            else:
                logger.warning(
                    "Admin user authentication failed - attempting to create user"
                )
                # Try to create the admin user
                if self._create_admin_user():
                    logger.info(
                        "Admin user created successfully, retrying authentication"
                    )
                    token = self._get_initial_token()
                    if token:
                        logger.info(
                            "Admin user authentication successful after creation"
                        )
                        self._ensure_nessie_catalog(token)
                    else:
                        raise ValueError(
                            "Failed to authenticate even after creating admin user"
                        )
                else:
                    raise ValueError("Failed to create admin user")

        except Exception as e:
            logger.error(f"Failed to initialize Dremio: {e}")
            raise

    def _get_initial_token(self):
        """Get token for initial setup checks."""
        try:
            response = requests.post(self.login_endpoint, json=self.payload)
            if response.status_code == 200:
                token = response.json().get("token", "")
                return token if token else None
            else:
                logger.warning(
                    f"Initial token request failed. Status code: {response.status_code}"
                )
                return None
        except requests.RequestException as e:
            logger.warning(f"Initial token request failed: {e}")
            return None

    def _create_admin_user(self) -> bool:
        """Create admin user if it doesn't exist."""
        try:
            # Prepare user creation payload
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

            logger.info(
                f"Creating admin user: {user_payload['userName']} ({user_payload['email']})"
            )

            response = requests.put(
                self.bootstrap_endpoint,
                json=user_payload,
                headers={"Content-Type": "application/json"},
            )

            if response.status_code in [200, 201]:
                logger.info("Admin user created successfully")
                return True
            else:
                logger.error(
                    f"Failed to create admin user. Status code: {response.status_code}"
                )
                logger.error(f"Response: {response.text}")
                return False

        except requests.RequestException as e:
            logger.error(f"Request failed while creating admin user: {e}")
            return False

    def _ensure_nessie_catalog(self, token: str):
        """Ensure Nessie catalog exists, create if it doesn't."""
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
        """Check if Nessie catalog already exists."""
        try:
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(
                f"{self.source_endpoint}/by-path/nessie", headers=headers
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
        """Create Nessie catalog in Dremio."""
        payload = {
            "name": "nessie",
            "config": {
                "nessieEndpoint": "http://nessie:19120/api/v2",
                "nessieAuthType": "NONE",
                "storageProvider": "AWS",
                "awsRootPath": "warehouse",
                "credentialType": "ACCESS_KEY",
                "awsAccessKey": "minioadmin",
                "awsAccessSecret": "miniopassword",
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
                self.apiv2_endpoint + "/source/nessie", json=payload, headers=headers
            )

            if response.status_code in [200, 201]:
                return True
            else:
                logger.error(
                    f"Failed to create Nessie catalog. Status code: {response.status_code}"
                )
                logger.error(f"Response: {response.text}")
                return False

        except requests.RequestException as e:
            logger.error(f"Request failed while creating Nessie catalog: {e}")
            return False
