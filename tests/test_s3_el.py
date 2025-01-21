from pathlib import Path

import pandas as pd
import pytest
from duckdb import DuckDBPyRelation
from minio import Minio
from utils import s3_el


TEST_DATA_DIR = Path(__file__).parent.joinpath("data")

client = Minio(
    s3_el.S3_ENDPOINT.replace("http://", ""),
    access_key=s3_el.AWS_ACCESS_KEY,
    secret_key=s3_el.AWS_SECRET_KEY,
    secure=False,
)


@pytest.fixture(autouse=True, scope="module")
def s3_bucket():
    client.make_bucket("securities-datalake")
    yield
    objs = [
        obj.object_name
        for obj in client.list_objects("securities-datalake", recursive=True)
    ]
    for obj in objs:
        client.remove_object("securities-datalake", obj)
    client.remove_bucket("securities-datalake")


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_symbols_data(asset_category):
    symbols = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.csv")
    )

    s3_el.load.fn(symbols, "symbols", asset_category)

    s3_objects = client.list_objects("securities-datalake", recursive=True)
    s3_objects = [obj.object_name for obj in s3_objects]
    assert f"symbols/{asset_category}.csv" in s3_objects


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_extract_symbols(asset_category):
    expected_symbols = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.csv")
    )["symbol"].to_list()

    symbols = s3_el.extract.fn("symbols", asset_category)

    assert symbols == expected_symbols


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_price_data(asset_category):
    price_df = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv")
    )

    s3_el.load.fn(price_df, "price_history", asset_category)

    s3_objects = client.list_objects("securities-datalake", recursive=True)
    s3_objects = [obj.object_name for obj in s3_objects]
    assert f"price_history/{asset_category}.parquet" in s3_objects


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_extract_price_data_returns_duckdb_relation(asset_category):
    price_history = s3_el.extract.fn("price_history", asset_category)

    assert isinstance(price_history, DuckDBPyRelation)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_extract_price_data_returns_expected_price_df(asset_category):
    expected_price_df = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv")
    )

    price_df = s3_el.extract.fn("price_history", asset_category).df()

    pd.testing.assert_frame_equal(price_df, expected_price_df)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_update_price_data(asset_category):
    hist_price_df = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv")
    )
    price_update = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"{asset_category}_price_data_update.csv")
    )
    expected_df = (
        pd.concat([hist_price_df, price_update], ignore_index=True)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )

    s3_el.load.fn(price_update, "price_history", asset_category)
    price_df = (
        s3_el.extract.fn("price_history", asset_category)
        .df()
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(price_df, expected_df)
