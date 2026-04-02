"""Catalog-agnostic PyIceberg catalog factory.

Provides a single factory function to create PyIceberg Catalog instances
for any supported backend (Lakekeeper, Nessie, PostgreSQL, AWS Glue).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, get_args

from loguru import logger
from pyiceberg.catalog import load_catalog

from poor_man_lakehouse.config import settings

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

LakehouseCatalogType = Literal["nessie", "lakekeeper", "postgres", "glue"]

_VALID_CATALOG_TYPES: set[str] = set(get_args(LakehouseCatalogType))


def get_catalog(
    catalog_type: LakehouseCatalogType | None = None,
) -> Catalog:
    """Create a PyIceberg Catalog for the specified or configured catalog type.

    Args:
        catalog_type: Catalog backend to use. Defaults to settings.CATALOG.

    Returns:
        A configured PyIceberg Catalog instance.

    Raises:
        ValueError: If the catalog type is not supported.
    """
    resolved_type = catalog_type or settings.CATALOG
    if resolved_type not in _VALID_CATALOG_TYPES:
        raise ValueError(f"Unsupported catalog type: '{resolved_type}'. Supported: {sorted(_VALID_CATALOG_TYPES)}")

    catalog_name = settings.CATALOG_NAME

    if resolved_type == "glue":
        config = _build_glue_config()
    elif resolved_type == "postgres":
        config = _build_postgres_config()
    else:
        config = _build_rest_config(resolved_type)

    logger.debug(f"Creating PyIceberg catalog '{catalog_name}' (type={resolved_type})")
    return load_catalog(catalog_name, **config)


def _build_rest_config(catalog_type: str) -> dict[str, str]:
    """Build config for REST catalogs (lakekeeper, nessie)."""
    uri_map: dict[str, str] = {
        "lakekeeper": settings.LAKEKEEPER_SERVER_URI,
        "nessie": settings.NESSIE_REST_URI,
    }
    return settings.ICEBERG_STORAGE_OPTIONS | {
        "type": "rest",
        "uri": uri_map[catalog_type],
    }


def _build_postgres_config() -> dict[str, str]:
    """Build config for PostgreSQL SQL catalog."""
    uri = (
        f"postgresql+psycopg://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
        f"@{settings.POSTGRES_HOST}/{settings.POSTGRES_DB}"
    )
    return settings.ICEBERG_STORAGE_OPTIONS | {
        "type": "sql",
        "uri": uri,
    }


def _build_glue_config() -> dict[str, str]:
    """Build config for AWS Glue catalog."""
    config: dict[str, str] = {
        "type": "glue",
        "s3.region": settings.AWS_DEFAULT_REGION,
        "warehouse": settings.WAREHOUSE_BUCKET.replace("s3a://", "s3://"),
    }
    if settings.GLUE_CATALOG_ID:
        config["glue.id"] = settings.GLUE_CATALOG_ID
    return config
