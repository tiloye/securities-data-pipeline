from pathlib import Path

import pandas as pd
import pytest
from minio import Minio

from pipeline.load import load
from pipeline.extract import get_prices_from_s3
from pipeline.config import (
    BUCKET_NAME,
    S3_ENDPOINT,
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
)

TEST_DATA_DIR = Path(__file__).parent.joinpath("data")

client = Minio(
    S3_ENDPOINT.replace("http://", ""),
    access_key=AWS_ACCESS_KEY,
    secret_key=AWS_SECRET_KEY,
    secure=False,
)


@pytest.fixture(autouse=True, scope="module")
def s3_bucket():
    client.make_bucket(BUCKET_NAME)
    yield
    objs = [obj.object_name for obj in client.list_objects(BUCKET_NAME, recursive=True)]
    for obj in objs:
        client.remove_object(BUCKET_NAME, obj)
    client.remove_bucket(BUCKET_NAME)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_symbols_data(asset_category):
    symbols = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.csv")
    )

    load(symbols, "symbols", asset_category)

    s3_objects = client.list_objects(BUCKET_NAME, recursive=True)
    s3_objects = [obj.object_name for obj in s3_objects]
    assert f"symbols/{asset_category}.csv" in s3_objects


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_price_data(asset_category):
    price_df = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv")
    )

    load(price_df, "price_history", asset_category)

    s3_objects = client.list_objects(BUCKET_NAME, recursive=True)
    s3_objects = [obj.object_name for obj in s3_objects]
    assert f"price_history/{asset_category}.parquet" in s3_objects


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_update_price_data(asset_category):
    # Load historical price
    hist_price_df = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv")
    )
    load(hist_price_df, "price_history", asset_category)

    # Load price update
    price_update = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"{asset_category}_price_data_update.csv")
    )
    load(price_update, "price_history", asset_category)

    # Verify merged data
    expected_df = (
        pd.concat([hist_price_df, price_update], ignore_index=True)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )

    loaded_price_df = get_prices_from_s3(asset_category)

    pd.testing.assert_frame_equal(loaded_price_df, expected_df)


if __name__ == "__main__":
    pytest.main([__file__])
