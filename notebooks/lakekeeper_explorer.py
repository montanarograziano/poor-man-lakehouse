import marimo

__generated_with = "0.19.5"
app = marimo.App(width="full")


@app.cell
def _():
    import os

    import marimo as mo

    lakekeeper_uri = os.getenv("LAKEKEEPER_SERVER_URI", "http://lakekeeper:8181")
    catalog_endpoint = f"{lakekeeper_uri}/catalog"
    print("Catalog endpoint:", catalog_endpoint)
    return catalog_endpoint, mo


@app.cell
def _(catalog_endpoint, mo):
    _df = mo.sql(
        f"""
        ATTACH OR REPLACE 'warehouse' AS warehouse (
            TYPE iceberg,
            ENDPOINT '{catalog_endpoint}',
            TOKEN ''
        );
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        """
        SELECT * FROM warehouse.default.bhoo;
        """
    )
    return


if __name__ == "__main__":
    app.run()
