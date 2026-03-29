# Dremio Connection

`DremioConnection` provides query federation via Arrow Flight protocol against a Dremio instance connected to Nessie.

!!! note "Requires Dremio + Nessie"
    Start with: `just up dremio` (starts Core + Nessie + Dremio)

## Basic Usage

```python
from poor_man_lakehouse import DremioConnection

with DremioConnection() as conn:
    # Query and get Polars DataFrame
    df = conn.to_polars("SELECT * FROM nessie.default.users")

    # Or DuckDB relation
    rel = conn.to_duckdb("SELECT * FROM nessie.default.users")

    # Or Pandas DataFrame
    pdf = conn.to_pandas("SELECT * FROM nessie.default.users")

    # Or raw Arrow stream
    stream = conn.to_arrow("SELECT * FROM nessie.default.users")
```

## Automatic Setup

On first connection, `DremioConnection` automatically:

1. Creates the admin user (if not exists)
2. Authenticates and obtains a PAT token
3. Creates the Nessie catalog source in Dremio (if not exists)
4. Initializes the Arrow Flight client

## Switching Catalogs

```python
conn.set_catalog("another_catalog")
```

## Output Formats

| Method | Returns | Best For |
|--------|---------|----------|
| `to_polars(query)` | `pl.DataFrame` | Polars-native analysis |
| `to_duckdb(query)` | DuckDB relation | Further SQL on results |
| `to_pandas(query)` | `pd.DataFrame` | Pandas ecosystem |
| `to_arrow(query)` | `FlightStreamReader` | Streaming/large results |

## Connection Lifecycle

```python
# Context manager (recommended)
with DremioConnection() as conn:
    df = conn.to_polars("SELECT 1")

# Manual cleanup
conn = DremioConnection()
try:
    df = conn.to_polars("SELECT 1")
finally:
    conn.close()
```
