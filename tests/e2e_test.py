from pathlib import Path

import pandas as pd
import pytest
from minio import Minio

from pipeline.config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    BUCKET_NAME,
    DATA_PATH,
    S3_ENDPOINT,
)
from pipeline.extract import YF_ERRORS
from pipeline.main import etl_bars, etl_fx_symbols, etl_sp_stocks_symbols

FX_SYMBOLS = ["EURUSD=X", "GBPUSD=X", "AUDUSD=X", "NZDUSD=X", "JPY=X", "CHF=X", "CAD=X"]
SP_SYMBOLS = ["AAPL", "INVALID_SYMBOL_1", "MSFT", "BRK-A", "BRK-B", "INVALID_SYMBOL_2"]

TEST_DATA_DIR = Path(__file__).parent.joinpath("data")


client = Minio(
    S3_ENDPOINT.replace("http://", ""),
    access_key=AWS_ACCESS_KEY,
    secret_key=AWS_SECRET_KEY,
    secure=False,
)

s3_storage_options = {
    "key": AWS_ACCESS_KEY,
    "secret": AWS_SECRET_KEY,
    "endpoint_url": S3_ENDPOINT,
}


@pytest.fixture(autouse=True, scope="module")
def s3_bucket():
    client.make_bucket(BUCKET_NAME)
    yield
    objs = [obj.object_name for obj in client.list_objects(BUCKET_NAME, recursive=True)]
    for obj in objs:
        client.remove_object(BUCKET_NAME, obj)
    client.remove_bucket(BUCKET_NAME)


########## Tests for Symbols ETL ##############


def test_etl_fx_symbols():
    expected_data = (
        pd.read_csv(TEST_DATA_DIR.joinpath("processed_fx_symbols.csv"))
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    etl_fx_symbols()
    loaded_data = (
        pd.read_csv(
            f"{DATA_PATH}/symbols/fx.csv",
            storage_options=s3_storage_options,
        )
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(loaded_data, expected_data)


def test_etl_sp_symbols(monkeypatch):
    symbols_df = pd.read_csv(TEST_DATA_DIR.joinpath("raw_sp_symbols.csv"))
    monkeypatch.setattr(
        "pipeline.main.get_sp_stock_symbols_from_source", lambda: symbols_df
    )

    etl_sp_stocks_symbols()
    loaded_data = (
        pd.read_csv(
            f"{DATA_PATH}/symbols/sp_stocks.csv",
            storage_options=s3_storage_options,
        )
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    expected_data = (
        pd.read_csv(TEST_DATA_DIR.joinpath("processed_sp_stocks_symbols.csv"))
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(loaded_data, expected_data)


########## Tests for Price History ETL ##############
@pytest.fixture
def price_data():
    def _price_data(asset_category, symbols=None):
        data = pd.read_csv(
            TEST_DATA_DIR.joinpath(f"raw_{asset_category}_prices.csv"),
            header=[0, 1],
            index_col=[0],
            parse_dates=True,
        )
        return data[symbols] if symbols else data

    return _price_data


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_etl_bars(monkeypatch, price_data, asset_category):
    expected_data = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv"),
        parse_dates=True,
    )
    expected_data["date"] = pd.to_datetime(expected_data["date"])
    symbols = ["PLACE_HOLDER"]

    monkeypatch.setattr(
        "pipeline.extract.yf.download",
        lambda *args, **kwargs: price_data(asset_category),
    )
    if asset_category == "sp_stocks":
        monkeypatch.setitem
        monkeypatch.setitem(
            YF_ERRORS, "sp_stocks", ["INVALID_SYMBOL_1", "INVALID_SYMBOL_2"]
        )

    etl_bars(asset_category, symbols, "2000-01-01", "2000-01-05")
    loaded_data = pd.read_parquet(
        f"{DATA_PATH}/price_history/{asset_category}.parquet",
        storage_options=s3_storage_options,
    )

    pd.testing.assert_frame_equal(loaded_data, expected_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_etl_bars_in_chunk(monkeypatch, price_data, asset_category):
    monkeypatch.setattr(
        "pipeline.extract.yf.download",
        lambda *args, **kwargs: price_data(asset_category, args[0]),
    )
    if asset_category == "sp_stocks":
        monkeypatch.setitem(
            YF_ERRORS, "sp_stocks", ["INVALID_SYMBOL_1", "INVALID_SYMBOL_2"]
        )

    symbols = FX_SYMBOLS if asset_category == "fx" else SP_SYMBOLS
    etl_bars(asset_category, symbols, "2000-01-01", "2000-01-05", chunk_size=2)

    loaded_data = (
        pd.read_parquet(
            f"s3://{BUCKET_NAME}/price_history/{asset_category}.parquet",
            storage_options=s3_storage_options,
        )
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )
    expected_data = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv"),
        parse_dates=True,
    )
    expected_data["date"] = pd.to_datetime(expected_data["date"])
    expected_data.sort_values(["date", "symbol"], inplace=True)
    expected_data.reset_index(drop=True, inplace=True)

    pd.testing.assert_frame_equal(loaded_data, expected_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_etl_bars_raises_exception(monkeypatch, asset_category):
    """
    Tests whether etl_bars function raises exception when tranformation function
    returns emtpy data frame.
    """

    monkeypatch.setattr(
        "pipeline.extract.yf.download", lambda *args, **kwargs: pd.DataFrame()
    )

    with pytest.raises(ValueError):
        symbols = ["PLACE_HOLDER"]
        etl_bars(asset_category, symbols, "2000-01-01", "2000-01-05")


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_etl_bars_in_chunk_raises_exception(monkeypatch, asset_category):
    """
    Tests whether etl_bars function raises exception when tranformation function
    returns empty data frame.
    """
    symbols = [
        "INVALID_SYMBOL_1",
        "INVALID_SYMBOL_2",
        "INVALID_SYMBOL_3",
        "INVALID_SYMBOL_4",
    ]
    monkeypatch.setattr(
        "pipeline.extract.yf.download", lambda *args, **kwargs: pd.DataFrame()
    )
    monkeypatch.setitem(YF_ERRORS, asset_category, symbols)

    with pytest.raises(ValueError):
        etl_bars(asset_category, symbols, "2000-01-01", "2000-01-05", chunk_size=2)


if __name__ == "__main__":
    pytest.main([__file__])
