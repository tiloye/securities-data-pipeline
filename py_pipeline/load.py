import duckdb
import pandas as pd
from fsspec import filesystem
from prefect import task

from py_pipeline.config import (
    DATA_PATH,
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    S3_ENDPOINT,
    DB_ENGINE,
)
from py_pipeline.validate import (
    transformed_stock_symbols_schema,
    transformed_fx_symbols_schema,
    transformed_price_schema,
)

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
            df = (
                transformed_stock_symbols_schema(df, lazy=True)
                if asset_category == "sp_stocks"
                else transformed_fx_symbols_schema(df, lazy=True)
            )
            merged_data = duckdb.sql(
                (
                    f"""
                    SELECT * 
                    FROM '{path}.parquet' 
                    UNION 
                    SELECT * 
                    FROM df 
                    ORDER BY symbol{", date_stamp" if asset_category == "sp_stocks" else ""}
                    """
                )
            )
        else:
            df = transformed_price_schema.validate(df, lazy=True)
            merged_data = duckdb.sql(
                f"SELECT * FROM '{path}.parquet' UNION SELECT * FROM df ORDER BY date, symbol"
            )
    except duckdb.IOException:
        duckdb.sql(f"COPY df TO '{path}.parquet' (FORMAT PARQUET)")
    else:
        duckdb.sql(f"COPY merged_data TO '{path}.parquet' (FORMAT PARQUET)")


@task
def load_to_dw(df: pd.DataFrame, dataset: str, asset_category: str) -> None:
    """
    Load price or symbols data into data warehouse.
    """

    if dataset not in ["symbols", "price_history"]:
        raise ValueError(f"Unknown dataset, {asset_category}")

    engine = DB_ENGINE
    table_name = f"{dataset}_{asset_category}"

    if dataset == "symbols":
        lookup_cols = (
            ["symbol", "date_stamp"] if asset_category == "sp_stocks" else ["symbol"]
        )
    else:
        lookup_cols = ["date", "symbol"]

    try:
        existing_data_symbols_df = pd.read_sql(
            f"""
            SELECT {",".join(lookup_cols)}
            FROM {table_name}
            """,
            con=engine,
        )
    except pd.errors.DatabaseError:
        df.to_sql(table_name, index=False, con=engine, if_exists="append")
    else:
        if dataset == "symbols" and asset_category == "fx":
            df.to_sql(table_name, index=False, con=engine, if_exists="replace")
        else:
            mask = ~df.set_index(lookup_cols).index.isin(
                existing_data_symbols_df.set_index(lookup_cols).index
            )
            df = df[mask]
            df.to_sql(table_name, index=False, con=engine, if_exists="append")
