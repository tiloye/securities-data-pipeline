import os

import duckdb
from dotenv import load_dotenv
from fsspec import filesystem
from pandas import DataFrame
from prefect import task

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
DATA_PATH = "s3://securities-datalake"

s3_storage_options = {
    "key": AWS_ACCESS_KEY,
    "secret": AWS_SECRET_KEY,
    "endpoint_url": S3_ENDPOINT,
}
duckdb.register_filesystem(filesystem("s3", **s3_storage_options))


@task
def extract(dataset: str, asset_category: str) -> list[str] | duckdb.DuckDBPyRelation:
    """Extract symbols or historical data from the object store."""

    path = f"{DATA_PATH}/{dataset}/{asset_category}"
    if dataset == "symbols":
        data = duckdb.sql(f"SELECT symbol FROM '{path}.csv'").df().squeeze().to_list()
        return data
    elif dataset == "price_history":
        data = duckdb.sql(f"SELECT * FROM '{path}.parquet'")
        return data
    else:
        raise ValueError(f"Unknown dataset, {dataset}")


@task
def load(df: DataFrame, dataset: str, asset_category: str) -> None:
    """Loads price data into an S3 bucket."""

    path = f"{DATA_PATH}/{dataset}/{asset_category}"
    if dataset == "symbols":
        duckdb.sql(f"COPY df TO '{path}.csv' (FORMAT CSV)")
    elif dataset == "price_history":
        try:
            merged_data = duckdb.sql(
                f"SELECT * FROM '{path}.parquet' UNION SELECT * FROM df"
            )
            duckdb.sql(f"COPY merged_data TO '{path}.parquet' (FORMAT PARQUET)")
        except duckdb.IOException:
            duckdb.sql(f"COPY df TO '{path}.parquet' (FORMAT PARQUET)")
    else:
        raise ValueError(f"Unknown dataset, {dataset}")
