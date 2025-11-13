import duckdb
from fsspec import filesystem
from pandas import DataFrame

from .config import DATA_PATH, AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_ENDPOINT

s3_storage_options = {
    "key": AWS_ACCESS_KEY,
    "secret": AWS_SECRET_KEY,
    "endpoint_url": S3_ENDPOINT,
}
duckdb.register_filesystem(filesystem("s3", **s3_storage_options))


def load(df: DataFrame, dataset: str, asset_category: str) -> None:
    """Load price or symbols data into an S3 bucket."""

    path = f"{DATA_PATH}/{dataset}/{asset_category}"
    if dataset == "symbols":
        df.to_csv(f"{path}.csv", index=False, storage_options=s3_storage_options)
    elif dataset == "price_history":
        try:
            merged_data = duckdb.sql(
                f"SELECT * FROM '{path}.parquet' UNION SELECT * FROM df ORDER BY date, symbol"
            )
            duckdb.sql(f"COPY merged_data TO '{path}.parquet' (FORMAT PARQUET)")
        except duckdb.IOException:
            duckdb.sql(f"COPY df TO '{path}.parquet' (FORMAT PARQUET)")
    else:
        raise ValueError(f"Unknown dataset, {dataset}")
