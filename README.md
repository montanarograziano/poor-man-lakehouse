# poor-man-lakehouse
 Attempt at implementing a local OSS lakehouse with tools like Polars, DuckDB, Delta, Iceberg and Minio.

## Setup

Copy `.env_template` into `.env` using `cp .env_template .env`

Run `just up` to spin Docker Compose services. Once everything is up run the following steps:

1. Create a `warehouse` bucket inside Minio
2. Configure Dremio attaching Nessie Catalog following [this](https://www.dremio.com/blog/intro-to-dremio-nessie-and-apache-iceberg-on-your-laptop/)  guide.
3. Run one of the notebooks to play around!


## Missing features

- [ ] Interact correctly between Unity Catalog and Docker pyspark
- [X] Setup Nessie for PyIceberg, Dremio and Pyspark
- [X] Add missing healthchecks for containers
- [X] Provide interface to select desired Catalog (ad-hoc install of pyspark jars, configs etc.)
- [X] Test Ibis for handling multiple backend engines
- [ ] Add frontend Docker Image for Unity Catalog

Currently, notebooks explains the experiments done so far with various engines and catalogs. Unity Catalog seems the most promising, but the current setting is still not complete.
