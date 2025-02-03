from pathlib import Path

import pandas as pd
import pytest
from minio import Minio
from prefect.testing.utilities import prefect_test_harness

from pipeline import price_history as ph
from utils import s3_el


FX_SYMBOLS = ["EURUSD=X", "GBPUSD=X", "AUDUSD=X", "NZDUSD=X", "JPY=X", "CHF=X", "CAD=X"]
SP_SYMBOLS = ["AAPL", "INVALID_SYMBOL_1", "MSFT", "BRK-A", "BRK-B", "INVALID_SYMBOL_2"]

TEST_DATA_DIR = Path(__file__).parent.joinpath("data")
s3_el.BUCKET_NAME = "test-" + s3_el.BUCKET_NAME
s3_el.DATA_PATH = f"s3://{s3_el.BUCKET_NAME}"


client = Minio(
    s3_el.S3_ENDPOINT.replace("http://", ""),
    access_key=s3_el.AWS_ACCESS_KEY,
    secret_key=s3_el.AWS_SECRET_KEY,
    secure=False,
)


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


@pytest.fixture(autouse=True, scope="module")
def s3_bucket():
    client.make_bucket(s3_el.BUCKET_NAME)
    yield
    objs = [
        obj.object_name
        for obj in client.list_objects(s3_el.BUCKET_NAME, recursive=True)
    ]
    for obj in objs:
        client.remove_object(s3_el.BUCKET_NAME, obj)
    client.remove_bucket(s3_el.BUCKET_NAME)


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
        ph.yf, "download", lambda *args, **kwargs: price_data(asset_category)
    )
    if asset_category == "sp_stocks":
        monkeypatch.setitem(
            ph.YF_ERRORS, "sp_stocks", ["INVALID_SYMBOL_1", "INVALID_SYMBOL_2"]
        )

    ph.etl_bars(asset_category, symbols, "2000-01-01", "2000-01-05")
    loaded_data = pd.read_parquet(
        f"s3://{s3_el.BUCKET_NAME}/price_history/{asset_category}.parquet",
        storage_options=s3_el.s3_storage_options,
    )

    pd.testing.assert_frame_equal(loaded_data, expected_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_etl_bars_in_chunk(monkeypatch, price_data, asset_category):
    expected_data = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv"),
        parse_dates=True,
    )
    expected_data["date"] = pd.to_datetime(expected_data["date"])
    expected_data.sort_values(["date", "symbol"], inplace=True)
    expected_data.reset_index(drop=True, inplace=True)

    symbols = FX_SYMBOLS if asset_category == "fx" else SP_SYMBOLS

    monkeypatch.setattr(
        ph.yf, "download", lambda *args, **kwargs: price_data(asset_category, args[0])
    )
    if asset_category == "sp_stocks":
        monkeypatch.setitem(
            ph.YF_ERRORS, "sp_stocks", ["INVALID_SYMBOL_1", "INVALID_SYMBOL_2"]
        )

    ph.etl_bars(asset_category, symbols, "2000-01-01", "2000-01-05", chunk_size=2)
    loaded_data = (
        pd.read_parquet(
            f"s3://{s3_el.BUCKET_NAME}/price_history/{asset_category}.parquet",
            storage_options=s3_el.s3_storage_options,
        )
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(loaded_data, expected_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_etl_bars_raises_exception(monkeypatch, asset_category):
    """Tests whether etl_bars function raises exception when tranformation function returns None."""

    symbols = ["PLACE_HOLDER"]

    monkeypatch.setattr(ph.yf, "download", lambda *args, **kwargs: pd.DataFrame())

    with pytest.raises(ValueError):
        ph.etl_bars(asset_category, symbols, "2000-01-01", "2000-01-05")


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_etl_bars_in_chunk_raises_exception(monkeypatch, asset_category):
    """Tests whether etl_bars function raises exception when tranformation function returns None."""

    symbols = [
        "INVALID_SYMBOL_1",
        "INVALID_SYMBOL_2",
        "INVALID_SYMBOL_3",
        "INVALID_SYMBOL_4",
    ]

    monkeypatch.setattr(ph.yf, "download", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setitem(ph.YF_ERRORS, asset_category, symbols)

    with pytest.raises(ValueError):
        ph.etl_bars(asset_category, symbols, "2000-01-01", "2000-01-05", chunk_size=2)
