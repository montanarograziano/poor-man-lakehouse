# Docker Services

All infrastructure runs via Docker Compose with profile-based service selection.

!!! warning "One Catalog at a Time"
    Multiple catalogs share the same PostgreSQL database and MinIO warehouse. Running them simultaneously causes metadata conflicts. The `full` profile is for testing only.

## Profiles

| Profile | Services | Ports |
|---------|----------|-------|
| *(none)* | MinIO, PostgreSQL | 9000, 9001, 5432 |
| `nessie` | + Nessie | + 19120 |
| `lakekeeper` | + Lakekeeper + bootstrap + warehouse init | + 8181 |
| `spark` | + Spark Master + Worker | + 7077, 8081, 8082 |
| `full` | All of the above | All ports |

## Usage

```bash
# Start with a specific catalog
just up lakekeeper

# Combine profiles (catalog + Spark)
docker compose --profile lakekeeper --profile spark up -d

# Stop everything
just down

# Clean restart (wipes all data)
just up-clean lakekeeper
```

## Core Services

### MinIO (S3-compatible Object Storage)

- **Image**: `minio/minio`
- **Ports**: 9000 (API), 9001 (Console)
- **Console**: http://localhost:9001
- **Health check**: HTTP on `/minio/health/live`
- **Data**: `./configs/minio/data`

A `minio-init-bucket` sidecar creates the `warehouse` bucket on startup.

### PostgreSQL

- **Image**: `postgres:17.5`
- **Port**: 5432
- **Health check**: `pg_isready`
- **Data**: `./configs/postgres/data`

Used as the metadata backend for Nessie and Lakekeeper.

## Catalog Services

### Nessie

- **Image**: `ghcr.io/projectnessie/nessie:0.107.2-java`
- **Port**: 19120
- **Backend**: PostgreSQL (JDBC2 version store)
- **Features**: Git-like versioning, REST + Iceberg API

### Lakekeeper

- **Image**: `quay.io/lakekeeper/catalog:latest-main`
- **Port**: 8181
- **Backend**: PostgreSQL
- **Initialization**: Migration, bootstrap, and warehouse creation via sidecar containers

## Compute Services

### Spark Cluster

See [Spark Cluster](spark-cluster.md) for details.

## Data Persistence

Service data is stored in `./configs/<service>/data/`. To wipe everything:

```bash
just up-clean nessie   # stops, removes volumes, deletes data dirs, restarts
```

Or manually:

```bash
just down
find ./configs -type d -name "data" -exec rm -rf {} +
just up nessie
```
