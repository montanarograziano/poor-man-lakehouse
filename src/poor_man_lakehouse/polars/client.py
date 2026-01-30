"""Polars client for Unity Catalog Delta tables."""

from dataclasses import dataclass

import polars as pl
import sqlglot

from poor_man_lakehouse.config import settings


@dataclass
class TableInfo:
    """Information about a table in the catalog."""

    catalog: str
    namespace: str
    name: str
    location: str | None = None

    @property
    def full_name(self) -> str:
        """Get the fully qualified table name."""
        return f"{self.catalog}.{self.namespace}.{self.name}"


class PolarsClient:
    """Interface similar to Spark and DuckDB exposing sql, but going through UC catalog.

    Example usage:
        client = PolarsClient(settings.UNITY_CATALOG_URI)

        # Explore the catalog
        print(client.list_catalogs())
        print(client.list_namespaces("unity"))
        print(client.list_tables("unity", "default"))

        # Get table schema
        schema = client.get_table_schema("unity", "default", "test_table")
        print(schema)

        # Run SQL queries
        df = client.sql("SELECT * FROM unity.default.test_table WHERE id > 10")

        # Lazy evaluation
        lazy_df = client.sql("SELECT * FROM unity.default.test_table", lazy=True)
        result = lazy_df.filter(pl.col("value") > 100).collect()
    """

    # Tables stored inside Docker volume that can't be loaded from outside
    DEFAULT_EXCLUDED_TABLES: set[str] = {
        "marksheet",
        "marksheet_uniform",
        "numbers",
        "user_countries",
    }

    def __init__(
        self,
        unity_catalog_uri: str | None = None,
        storage_options: dict | None = None,
        require_https: bool = False,
    ):
        """Initialize the Polars Unity Catalog client.

        Args:
            unity_catalog_uri: URI of the Unity Catalog server. Defaults to settings.UNITY_CATALOG_URI.
            storage_options: S3 storage options. Defaults to settings.S3_STORAGE_OPTIONS.
            require_https: Whether to require HTTPS for the catalog connection.
        """
        self.unity_catalog_uri = unity_catalog_uri or settings.UNITY_CATALOG_URI
        self.catalog = pl.Catalog(self.unity_catalog_uri, require_https=require_https)
        self.storage_options = storage_options or settings.S3_STORAGE_OPTIONS
        self._table_cache: dict[str, pl.LazyFrame] = {}

    def list_catalogs(self) -> list[str]:
        """List all available catalogs."""
        return [cat.name for cat in self.catalog.list_catalogs()]

    def list_namespaces(self, catalog: str) -> list[str]:
        """List all namespaces (schemas) in a catalog."""
        return [ns.name for ns in self.catalog.list_namespaces(catalog)]

    def list_tables(self, catalog: str, namespace: str) -> list[str]:
        """List all tables in a namespace."""
        return [tbl.name for tbl in self.catalog.list_tables(catalog, namespace)]

    def get_table_info(self, catalog: str, namespace: str, table: str) -> TableInfo:
        """Get detailed information about a table."""
        return TableInfo(
            catalog=catalog,
            namespace=namespace,
            name=table,
        )

    def get_table_schema(self, catalog: str, namespace: str, table: str) -> pl.Schema:
        """Get the schema of a table without loading data."""
        lf = self._scan_table(catalog, namespace, table)
        return lf.collect_schema()

    def scan_table(self, catalog: str, namespace: str, table: str) -> pl.LazyFrame:
        """Scan a table and return a LazyFrame for further processing.

        Useful for building complex queries with Polars expressions.
        """
        return self._scan_table(catalog, namespace, table)

    def _scan_table(self, catalog: str, namespace: str, table: str) -> pl.LazyFrame:
        """Internal method to scan a table with caching."""
        cache_key = f"{catalog}.{namespace}.{table}"
        if cache_key not in self._table_cache:
            self._table_cache[cache_key] = self.catalog.scan_table(
                catalog, namespace, table, storage_options=self.storage_options
            )
        return self._table_cache[cache_key]

    def clear_cache(self) -> None:
        """Clear the table scan cache."""
        self._table_cache.clear()

    def sql(self, query: str, lazy: bool = False) -> pl.DataFrame | pl.LazyFrame:
        """Run SQL query against Unity Catalog tables.

        Args:
            query: SQL query string with fully qualified table names (catalog.namespace.table)
            lazy: If True, return LazyFrame instead of collecting results

        Returns:
            DataFrame or LazyFrame depending on the lazy parameter
        """
        parsed = sqlglot.parse_one(query)
        tables = list(parsed.find_all(sqlglot.exp.Table))  # pyright: ignore[reportPrivateImportUsage]

        if not tables:
            raise ValueError("No tables found in query")

        # Build a mapping of original table references to LazyFrame variable names
        table_frames: dict[str, pl.LazyFrame] = {}

        for i, table in enumerate(tables):
            catalog_name = str(table.catalog) if table.catalog else None
            db_name = str(table.db) if table.db else None
            table_name = str(table.this)

            if not catalog_name or not db_name:
                raise ValueError(f"Table '{table_name}' must be fully qualified as catalog.namespace.table")

            var_name = f"_tbl_{i}"
            table_frames[var_name] = self._scan_table(catalog_name, db_name, table_name)

            # Replace the table reference in the parsed query
            table.set("catalog", None)
            table.set("db", None)
            table.set("this", sqlglot.exp.Identifier(this=var_name))  # pyright: ignore[reportPrivateImportUsage]

        # Generate the transformed SQL
        transformed_sql = parsed.sql()

        # Execute with SQLContext to register the LazyFrames
        ctx = pl.SQLContext(table_frames, eager=not lazy)
        return ctx.execute(transformed_sql)

    def show_catalog_tree(self, max_tables: int = 10) -> str:
        """Display a tree view of the catalog structure.

        Args:
            max_tables: Maximum number of tables to show per namespace

        Returns:
            String representation of the catalog tree
        """
        lines = []
        catalogs = self.list_catalogs()

        for cat in catalogs:
            lines.append(f"📁 {cat}")
            try:
                namespaces = self.list_namespaces(cat)
                for ns in namespaces:
                    lines.append(f"  📂 {ns}")
                    try:
                        tables = self.list_tables(cat, ns)
                        for j, tbl in enumerate(tables[:max_tables]):
                            prefix = "  └──" if j == len(tables[:max_tables]) - 1 else "  ├──"
                            lines.append(f"    {prefix} 📄 {tbl}")
                        if len(tables) > max_tables:
                            lines.append(f"    └── ... and {len(tables) - max_tables} more tables")
                    except Exception as e:
                        lines.append(f"    └── (error listing tables: {e})")
            except Exception as e:
                lines.append(f"  └── (error listing namespaces: {e})")

        return "\n".join(lines)

    def describe_table(self, catalog: str, namespace: str, table: str) -> pl.DataFrame:
        """Get a description of the table including column names, types, and nullability."""
        schema = self.get_table_schema(catalog, namespace, table)
        return pl.DataFrame(
            {
                "column": list(schema.names()),
                "dtype": [str(dtype) for dtype in schema.dtypes()],
            }
        )

    def load_all_tables(
        self,
        catalog: str | None = None,
        namespace: str | None = None,
        name_format: str = "flat",
        exclude_tables: set[str] | None = None,
    ) -> dict[str, pl.LazyFrame]:
        """Load all tables as LazyFrames.

        Args:
            catalog: Specific catalog to load from (default: all catalogs)
            namespace: Specific namespace to load from (default: all namespaces)
            name_format: How to name the tables:
                - "flat": table_name (may conflict if same name in different namespaces)
                - "namespace": namespace__table_name
                - "full": catalog__namespace__table_name
            exclude_tables: Set of table names to exclude (default: DEFAULT_EXCLUDED_TABLES)

        Returns:
            Dictionary of {table_name: LazyFrame}
        """
        excluded = exclude_tables if exclude_tables is not None else self.DEFAULT_EXCLUDED_TABLES
        tables_dict: dict[str, pl.LazyFrame] = {}

        catalogs_to_scan = [catalog] if catalog else self.list_catalogs()

        for cat in catalogs_to_scan:
            namespaces_to_scan = [namespace] if namespace else self.list_namespaces(cat)

            for ns in namespaces_to_scan:
                for tbl in self.list_tables(cat, ns):
                    if tbl in excluded:
                        continue

                    if name_format == "flat":
                        key = tbl
                    elif name_format == "namespace":
                        key = f"{ns}__{tbl}"
                    else:  # full
                        key = f"{cat}__{ns}__{tbl}"

                    tables_dict[key] = self._scan_table(cat, ns, tbl)

        return tables_dict

    def to_duckdb(
        self,
        catalog: str | None = None,
        namespace: str | None = None,
        exclude_tables: set[str] | None = None,
    ):
        """Create a DuckDB connection with all Unity Catalog tables registered.

        Args:
            catalog: Specific catalog to load from (default: all catalogs)
            namespace: Specific namespace to load from (default: all namespaces)
            exclude_tables: Set of table names to exclude (default: DEFAULT_EXCLUDED_TABLES)

        Returns:
            DuckDB connection with tables registered
        """
        import duckdb

        conn = duckdb.connect()
        tables = self.load_all_tables(catalog, namespace, name_format="full", exclude_tables=exclude_tables)

        for name, lf in tables.items():
            conn.register(name, lf.collect())

        return conn

    def __repr__(self) -> str:
        """String representation of the PolarsClient."""
        return f"PolarsClient(uri='{self.unity_catalog_uri}')"
