import duckdb
import pandas as pd
from fsspec import filesystem
from prefect import task
from sqlalchemy.engine import create_engine

from py_pipeline.config import DATA_PATH, AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_ENDPOINT, DB_ENGINE

s3_storage_options = {
    "key": AWS_ACCESS_KEY,
    "secret": AWS_SECRET_KEY,
    "endpoint_url": S3_ENDPOINT,
}
duckdb.register_filesystem(filesystem("s3", **s3_storage_options))


@task
def load_to_s3(df: pd.DataFrame, dataset: str, asset_category: str) -> None:
    """Load price or symbols data into an S3 bucket."""

    if dataset not in ["symbols", "price_history"]:
        raise ValueError(f"Unknown dataset, {asset_category}")

    path = f"{DATA_PATH}/{dataset}/{asset_category}"
    try:
        if dataset == "symbols":
            existing_data = duckdb.sql(
            f"SELECT * FROM '{path}.parquet' UNION SELECT * FROM df ORDER BY symbol, date_stamp"
        )
        else:
            existing_data = duckdb.sql(
                f"SELECT * FROM '{path}.parquet' UNION SELECT * FROM df ORDER BY date, symbol"
            )
        duckdb.sql(f"COPY existing_data TO '{path}.parquet' (FORMAT PARQUET)")
    except duckdb.IOException:
        duckdb.sql(f"COPY df TO '{path}.parquet' (FORMAT PARQUET)")


@task
def load_to_dw(df: pd.DataFrame, dataset: str, asset_category: str) -> None:
    engine = DB_ENGINE
    table_name = f"{dataset}_{asset_category}"

    if dataset == "symbols":
        df.to_sql(table_name, index=False, con=engine, if_exists="replace")
    elif dataset == "price_history":
        try:
            existing_date_symbols_df = pd.read_sql(
                f"SELECT date, symbol FROM {table_name}", con=engine
            )
            mask = ~df.set_index(["date", "symbol"]).index.isin(
                existing_date_symbols_df.set_index(["date", "symbol"]).index
            )
            df = df[mask]
            df.to_sql(table_name, index=False, con=engine, if_exists="append")
        except Exception:
            df.to_sql(table_name, index=False, con=engine, if_exists="append")
    else:
        raise ValueError(f"Unknown dataset, {dataset}")
