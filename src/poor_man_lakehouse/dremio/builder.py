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
        self.login_endpoint = f"{settings.DREMIO_SERVER_URI}/apiv2/login"
        self.catalog_name = settings.CATALOG_NAME

        ## Payload for Login
        self.payload = {
            "userName": settings.DREMIO_USERNAME,
            "password": settings.DREMIO_PASSWORD,
        }
        self.arrow_endpoint = f"{settings.ARROW_ENDPOINT}"
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

    ## Function to Retrieve PAT TOken from Dremio
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
            print("Failed to get a valid response. Status code:", response.status_code)
