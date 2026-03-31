# AWS Glue Catalog Integration Design

## Summary

Add AWS Glue Catalog as a fifth catalog backend for Poor Man's Lakehouse, supporting both Spark and DuckDB engines for reading and writing Iceberg tables.

## Scope

- **Spark connector**: New `GlueCatalogSparkBuilder` class
- **Ibis connector**: Extend `IbisConnection` DuckDB backend to support Glue catalog
- **Config**: Add `GLUE_CATALOG_ID` setting, `CatalogType.GLUE` enum value
- **No Docker changes**: Glue is a managed AWS service, no local emulation

## Design Decisions

1. **Iceberg only** (no Delta) — consistent with Postgres/Nessie/Lakekeeper builders
2. **AWS default credential chain** — no static S3 keys injected in Spark config. AWS SDK resolves credentials from: env vars > `~/.aws/credentials` > IAM roles. If `AWS_ACCESS_KEY_ID` is set as env var, it's picked up automatically.
3. **Real AWS only** — no LocalStack. Unit tests mock everything, no integration tests.
4. **DuckDB Glue support** via DuckDB's native `iceberg` extension Glue catalog type.

## Spark Connector: `GlueCatalogSparkBuilder`

- Catalog implementation: `org.apache.iceberg.aws.glue.GlueCatalog`
- Uses `iceberg-aws-bundle` JAR (already in `COMMON_PACKAGES`)
- Overrides `_configure_common()` to skip static S3 credential injection (like Lakekeeper/Unity)
- Sets `io-impl` to `org.apache.iceberg.aws.s3.S3FileIO`
- Configures `glue.id` if `GLUE_CATALOG_ID` is set (cross-account access)
- Uses `AWS_DEFAULT_REGION` for Glue API endpoint resolution
- Warehouse path from `WAREHOUSE_BUCKET` (real S3 bucket)

## Ibis Connector: DuckDB + Glue

- Relax `IbisConnection.__init__` to accept both `lakekeeper` and `glue` catalogs
- Add `_duckdb_glue_connection` path that configures DuckDB's iceberg extension with `TYPE glue`
- S3 credentials: use DuckDB's `credential_chain` provider for AWS default chain
- Glue catalog ID passed if configured

## Config Changes

- Add `GLUE_CATALOG_ID: str = ""` to `Settings`
- Add `CatalogType.GLUE = "glue"` to enum
- Wire `GlueCatalogSparkBuilder` into `_CATALOG_BUILDERS` dict
- Add Glue case to `_configure_data_path()` for `ICEBERG_STORAGE_OPTIONS`
- Update `CATALOG` field comment to include "glue"

## Testing

- Unit tests for `GlueCatalogSparkBuilder` configuration (mock-based)
- Unit tests for `IbisConnection` with Glue catalog
- Unit tests for new config fields and Glue storage options
- No integration tests (requires real AWS)

## Documentation

- CLAUDE.md: update architecture, catalog list, constraints
- README.md: add Glue section with setup and credential docs
- `.env.example`: add Glue-specific settings with documentation
- Docstrings: Google style on all new classes/methods
- `__init__.py`: update re-exports if needed
