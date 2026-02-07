import marimo

__generated_with = "0.19.6"
app = marimo.App(width="full")


@app.cell
def _():
    import time

    import marimo as mo
    import polars as pl

    from poor_man_lakehouse.polars_connector import PolarsClient

    # Initialize the client
    cl = PolarsClient()

    def sql_cell(
        query: str,
        display: bool = True,
        limit: int | None = 1000,
        page_size: int = 10,
    ) -> pl.DataFrame | mo.ui.table:
        """Databricks-style %sql cell for Unity Catalog queries.

        Execute SQL and display results in an interactive table, similar to Databricks notebooks.

        Args:
            query: SQL query with fully qualified table names (catalog.namespace.table)
            display: If True, returns an interactive table display (default: True)
            limit: Max rows to display (default: 1000, None for unlimited)
            page_size: Rows per page in the interactive table (default: 10)

        Returns:
            If display=True: mo.ui.table with interactive features
            If display=False: pl.DataFrame

        Example:
            # In a Marimo cell, just write:
            sql_cell('''
                SELECT * FROM unity.default.test_table
                WHERE value > 100
                ORDER BY id
            ''')
        """
        start = time.perf_counter()
        result = cl.sql(query)
        elapsed = time.perf_counter() - start

        # sql() with lazy=False (default) returns DataFrame
        if not isinstance(result, pl.DataFrame):
            raise TypeError("Expected polars.DataFrame from sql() with lazy=False")
        df = result

        # Apply limit if specified
        if limit is not None and len(df) > limit:
            df = df.head(limit)
            truncated = True
        else:
            truncated = False

        if not display:
            return df

        # Build status message
        row_count = len(df)
        status_parts = [f"{row_count:,} rows"]
        if truncated:
            status_parts.append(f"(limited to {limit:,})")
        status_parts.append(f"in {elapsed:.2f}s")
        status_msg = " ".join(status_parts)

        # Create interactive table with Marimo
        return mo.ui.table(
            df,
            pagination=True,
            page_size=page_size,
            selection=None,
            label=status_msg,
        )

    return cl, mo, pl, sql_cell


@app.cell
def _(cl):
    # Example: Explore the catalog structure
    print(cl.show_catalog_tree())
    return


@app.cell
def _(cl):
    # Example: Get table schema before querying
    cl.describe_table("unity", "default", "test_table")
    return


@app.cell
def _(cl):
    # Example: Run SQL query using the client
    cl.sql("SELECT * FROM unity.default.test_table")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Databricks-style SQL Cells

    Use `sql_cell()` for a Databricks-like `%sql` experience:
    - Write SQL with fully qualified table names (`catalog.namespace.table`)
    - Results displayed in an interactive, paginated table
    - Shows row count and execution time
    """)
    return


@app.cell
def _(sql_cell):
    # %sql - Databricks style! Just write your query
    sql_cell("""
        SELECT * FROM unity.default.test_table
    """)
    return


@app.cell
def _(sql_cell):
    # %sql - With filtering and ordering
    sql_cell("""
        SELECT *
        FROM unity.default.test_table
        ORDER BY country DESC
        LIMIT 100
    """)
    return


@app.cell
def _(sql_cell):
    # Get raw DataFrame instead of display (for further processing)
    df = sql_cell(
        "SELECT * FROM unity.default.test_table",
        display=False,
    )
    df.head()
    return


if __name__ == "__main__":
    app.run()
