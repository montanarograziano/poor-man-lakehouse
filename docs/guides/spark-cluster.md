# Spark Cluster

The project includes a Docker-based Spark standalone cluster for distributed processing.

## Starting the Cluster

```bash
# Spark cluster alone
just up spark

# Combined with a catalog
docker compose --profile lakekeeper --profile spark up -d
```

## Architecture

| Service | Image | Ports |
|---------|-------|-------|
| `spark-master` | `apache/spark:4.0.1-scala2.13-java21-python3-ubuntu` | 7077 (RPC), 8081 (Web UI) |
| `spark-worker` | Same | 8082 (Web UI) |

Both services mount `configs/spark/spark-defaults.conf` for shared configuration.

## Web UIs

- **Master UI**: http://localhost:8081
- **Worker UI**: http://localhost:8082

## Connecting from Python

Set these in your `.env`:

```dotenv
SPARK_MASTER="spark://localhost:7077"
SPARK_DRIVER_HOST="172.18.0.1"
SPARK_DRIVER_PORT=7001
SPARK_DRIVER_BLOCK_MANAGER_PORT=7002
```

!!! tip "Finding the Docker Bridge IP"
    ```bash
    docker network inspect bridge --format '{{range .IPAM.Config}}{{.Gateway}}{{end}}'
    ```

Then create a session:

```python
from poor_man_lakehouse import retrieve_current_spark_session

spark = retrieve_current_spark_session()
spark.sql("SHOW DATABASES").show()
```

## spark-defaults.conf

The cluster ships with a pre-configured `configs/spark/spark-defaults.conf` containing:

- Iceberg + Delta SQL extensions
- S3A filesystem configuration for MinIO
- JAR packages (resolved via Ivy on first use):
    - `iceberg-spark-runtime-4.0_2.13:1.10.1`
    - `iceberg-aws-bundle:1.10.1`
    - `hadoop-aws:3.4.1`
    - `postgresql:42.7.10`
    - `delta-spark_2.13:4.0.1`

## Local Mode (No Cluster)

For development without a cluster, set:

```dotenv
SPARK_MASTER="local[*]"
```

This runs Spark in-process using all available cores. No Docker Spark services needed.

## Health Checks

Both master and worker have health checks via their Web UIs:

```yaml
healthcheck:
  test: ["CMD-SHELL", "wget -q --spider http://localhost:8080/ || exit 1"]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 15s
```

The worker depends on the master being healthy before starting.
