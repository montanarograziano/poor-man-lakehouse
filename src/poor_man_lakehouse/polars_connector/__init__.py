"""Polars client for Unity Catalog with SQL magic support."""

from poor_man_lakehouse.polars_connector.client import PolarsClient

__all__ = ["PolarsClient", "load_sql_magic"]


def load_sql_magic(client: PolarsClient | None = None):
    """Load the %%sql cell magic for Jupyter notebooks.

    Args:
        client: Optional PolarsClient instance. If not provided, creates one with default settings.

    Example:
        In a Jupyter notebook:

        ```python
        from poor_man_lakehouse.polars_connector import PolarsClient, load_sql_magic

        client = PolarsClient("http://localhost:8080/")
        load_sql_magic(client)
        ```

        Then in any cell:

        ```
        %%sql
        SELECT * FROM unity.default.test_table
        WHERE value > 100
        ```
    """
    from poor_man_lakehouse.polars_connector.magic import load_ipython_extension, set_client

    if client is not None:
        set_client(client)

    # Get the current IPython instance and load the extension
    try:
        from IPython import get_ipython # pyright: ignore[reportPrivateImportUsage]

        ip = get_ipython()
        if ip is not None:
            load_ipython_extension(ip)
        else:
            raise RuntimeError("No IPython instance found. Are you running in a Jupyter notebook?")
    except ImportError:
        raise RuntimeError("IPython is not installed. Install it with: pip install ipython")
