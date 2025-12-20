import marimo

__generated_with = "0.17.7"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    from poor_man_lakehouse.config import settings

    return mo, settings


@app.cell
def _(mo, settings):
    _df = mo.sql(
        f"""
        INSTALL iceberg;
        INSTALL ducklake;
        INSTALL postgres;
        UPDATE EXTENSIONS;
        LOAD iceberg;

        CREATE OR REPLACE SECRET s3_secret (
                TYPE S3,
                KEY_ID '{settings.AWS_ACCESS_KEY_ID}',
                SECRET '{settings.AWS_SECRET_ACCESS_KEY}',
                ENDPOINT '{settings.AWS_ENDPOINT_URL}',
                URL_STYLE 'path',
                USE_SSL false
        );
        CREATE OR REPLACE SECRET postgres_secret (
                TYPE postgres,
                HOST '{settings.POSTGRES_HOST}',
                DATABASE '{settings.POSTGRES_DB}',
                USER '{settings.POSTGRES_USER}',
                PASSWORD '{settings.POSTGRES_PASSWORD}'
        );

        USE memory;
        DETACH DATABASE IF EXISTS my_ducklake;
        ATTACH OR REPLACE 'ducklake:postgres:dbname={settings.POSTGRES_DB} host={settings.POSTGRES_HOST} user={settings.POSTGRES_USER} password={settings.POSTGRES_PASSWORD}' AS my_ducklake
            (DATA_PATH '{settings.WAREHOUSE_BUCKET}ducklake/');
        USE my_ducklake;
        """
    )
    return


@app.cell
def _(mo, train_stations):
    _df = mo.sql(
        """
        SELECT * FROM train_stations
        """
    )
    return


if __name__ == "__main__":
    app.run()
