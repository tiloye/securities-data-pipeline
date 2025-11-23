from pathlib import Path

import pandas as pd
import pytest
from minio import Minio

from pipeline.extract import (
    get_symbols_from_s3,
    get_prices_from_s3,
    get_prices_from_source,
    log_failed_dowloads,
    yf,
    YF_ERRORS,
)
from pipeline.config import (
    BUCKET_NAME,
    S3_ENDPOINT,
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    DATA_PATH,
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
    # Create S3 bucket
    client.make_bucket(BUCKET_NAME)

    # Define S3 storage options
    s3_storage_options = {
        "key": AWS_ACCESS_KEY,
        "secret": AWS_SECRET_KEY,
        "endpoint_url": S3_ENDPOINT,
    }

    # Load symbols data into S3 bucket
    fx_symbols = pd.read_csv(TEST_DATA_DIR.joinpath("processed_fx_symbols.csv"))
    stock_symbols = pd.read_csv(
        TEST_DATA_DIR.joinpath("processed_sp_stocks_symbols.csv")
    )
    fx_symbol_path = f"{DATA_PATH}/symbols/fx.csv"
    stock_symbol_path = f"{DATA_PATH}/symbols/sp_stocks.csv"
    fx_symbols.to_csv(fx_symbol_path, index=False, storage_options=s3_storage_options)
    stock_symbols.to_csv(
        stock_symbol_path, index=False, storage_options=s3_storage_options
    )

    # Load price data into S3 bucket
    fx_price_data = pd.read_csv(TEST_DATA_DIR.joinpath("processed_fx_prices.csv"))
    fx_price_data["date"] = pd.to_datetime(fx_price_data["date"])

    stocks_price_data = pd.read_csv(
        TEST_DATA_DIR.joinpath("processed_sp_stocks_prices.csv")
    )
    stocks_price_data["date"] = pd.to_datetime(stocks_price_data["date"])

    fx_price_path = f"{DATA_PATH}/price_history/fx.parquet"
    stock_price_path = f"{DATA_PATH}/price_history/sp_stocks.parquet"

    fx_price_data.to_parquet(
        fx_price_path, index=False, storage_options=s3_storage_options
    )
    stocks_price_data.to_parquet(
        stock_price_path, index=False, storage_options=s3_storage_options
    )

    yield
    objs = [obj.object_name for obj in client.list_objects(BUCKET_NAME, recursive=True)]
    for obj in objs:
        client.remove_object(BUCKET_NAME, obj)
    client.remove_bucket(BUCKET_NAME)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_get_fx_symbols_from_s3(asset_category):
    expected_data = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.csv")
    )["symbol"].to_list()

    extracted_data = get_symbols_from_s3(asset_category)

    assert extracted_data == expected_data


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_get_prices_from_s3(asset_category):
    expected_data = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv"),
        parse_dates=["date"],
    )

    price_df = get_prices_from_s3(asset_category)

    pd.testing.assert_frame_equal(price_df, expected_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_get_prices_from_s3_by_date(asset_category):
    start_date = pd.Timestamp("2006-05-19")
    end_date = pd.Timestamp("2006-05-22")

    expected_data = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv")
    )
    expected_data["date"] = pd.to_datetime(expected_data["date"])
    mask = (expected_data["date"] >= start_date) & (expected_data["date"] <= end_date)
    expected_data = expected_data.loc[mask].reset_index(drop=True)

    price_df = get_prices_from_s3(
        asset_category,
        start=start_date,
        end=end_date,
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(price_df, expected_data)


@pytest.mark.parametrize(
    ("asset_category", "symbols"),
    (
        ("fx", ["INVALID_FX_SYMBOL_1", "INVALID_FX_SYMBOL_2"]),
        ("sp_stocks", ["INVALID_STOCK_SYMBOL_1", "INVALID_STOCK_SYMBOL_2"]),
    ),
)
def test_log_failed_downloads(monkeypatch, asset_category, symbols):
    monkeypatch.setattr(
        yf,
        "download",
        lambda symbols, start, end, group_by, auto_adjust: None,
    )  # Avoid sending request to Yahoo Finance
    monkeypatch.setitem(yf.shared._ERRORS, symbols[0], "Error message")
    monkeypatch.setitem(yf.shared._ERRORS, symbols[1], "Error message")

    data = get_prices_from_source(symbols)
    log_failed_dowloads(asset_category)

    assert YF_ERRORS[asset_category] == list(yf.shared._ERRORS.keys())


if __name__ == "__main__":
    pytest.main([__file__])
