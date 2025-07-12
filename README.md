# poor-man-lakehouse
 Attempt at implementing a local OSS lakehouse with tools like Polars, DuckDB, Delta, Iceberg and Minio.

## Missing features

- [ ] Interact correctly between Unity Catalog and Docker pyspark
- [ ] Add missing healthchecks for containers
- [X] Provide interface to select desired Catalog (ad-hoc install of pyspark jars, configs etc.)
- [ ] Test Ibis for handling multiple backend engines
- [ ] Add frontend Docker Image for Unity Catalog

Currently, notebooks explains the experiments done so far with various engines and catalogs. Unity Catalog seems the most promising, but the current setting is still not complete.
