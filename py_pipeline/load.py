import dlt
import pandas as pd

from py_pipeline.config import (
    DATA_PATH,
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    S3_ENDPOINT,
    DB_TYPE,
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_PASSWORD,
    DB_NAME,
)
from py_pipeline.validate import (
    transformed_stock_symbols_schema,
    transformed_fx_symbols_schema,
    transformed_price_schema,
)

dlt.config["load.delete_completed_jobs"] = True
dlt.config["load.truncate_staging_dataset"] = True


def load(
    df: pd.DataFrame,
    dataset: str,
    asset_category: str,
    destination: str = "s3",
) -> None:
    if destination == "s3":
        return load_to_s3(df, dataset, asset_category)
    elif destination == "dw":
        return load_to_dw(df, dataset, asset_category)
    else:
        raise ValueError(f"Unknown destination: {destination}")


def load_to_s3(df: pd.DataFrame, dataset: str, asset_category: str) -> None:
    """Load price or symbols data into an S3 bucket."""

    if dataset not in ["symbols", "price_history"]:
        raise ValueError(f"Unknown dataset, {asset_category}")

    write_disposition = "merge"

    if dataset == "symbols":
        primary_key = (
            ["symbol", "date_stamp"] if asset_category == "sp_stocks" else ["symbol"]
        )
        if asset_category == "fx":
            write_disposition = "replace"

        df = (
            transformed_stock_symbols_schema(df, lazy=True)
            if asset_category == "sp_stocks"
            else transformed_fx_symbols_schema(df, lazy=True)
        )
    else:
        # For price_history
        primary_key = ["date_stamp", "symbol"]
        df = transformed_price_schema.validate(df, lazy=True)

    pipeline = dlt.pipeline(
        pipeline_name=f"sec_s3_loader_{dataset}_{asset_category}",
        destination=dlt.destinations.filesystem(
            bucket_url=DATA_PATH,
            credentials={
                "aws_access_key_id": AWS_ACCESS_KEY,
                "aws_secret_access_key": AWS_SECRET_KEY,
                "endpoint_url": S3_ENDPOINT,
            },
        ),
        dataset_name=dataset,
    )

    load_info = pipeline.run(
        df,
        table_name=asset_category,
        write_disposition=write_disposition,
        primary_key=primary_key,
        table_format="delta",
    )

    print(load_info)


def load_to_dw(df: pd.DataFrame, dataset: str, asset_category: str) -> None:
    """
    Load price or symbols data into data warehouse.
    """

    if dataset not in ["symbols", "price_history"]:
        raise ValueError(f"Unknown dataset, {asset_category}")

    table_name = f"{dataset}_{asset_category}"
    write_disposition = "merge"

    if dataset == "symbols":
        primary_key = (
            ["symbol", "date_stamp"] if asset_category == "sp_stocks" else ["symbol"]
        )
        if asset_category == "fx":
            write_disposition = "replace"
    else:
        # For price_history
        primary_key = ["date_stamp", "symbol"]

    pipeline = dlt.pipeline(
        pipeline_name=f"sec_dw_loader_{dataset}_{asset_category}",
        destination=get_dw_destination(),
        dataset_name="public",
    )

    load_info = pipeline.run(
        df,
        table_name=table_name,
        write_disposition=write_disposition,
        primary_key=primary_key,
    )

    print(load_info)


def get_dw_destination():
    if DB_TYPE == "postgres":
        return dlt.destinations.postgres(
            f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
    elif DB_TYPE == "snowflake":
        return dlt.destinations.snowflake(
            credentials=f"snowflake://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}",
            keep_staged_files=False,
        )
    else:
        raise ValueError(f"Unknown database type: {DB_TYPE}")
