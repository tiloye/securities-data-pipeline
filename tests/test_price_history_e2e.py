from pathlib import Path

import pandas as pd
import pytest
from minio import Minio
from prefect.testing.utilities import prefect_test_harness

from pipeline import price_history as ph
from utils import s3_el


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


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_etl_bars(monkeypatch, asset_category):
    expected_data = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv"),
        parse_dates=True,
    )
    expected_data["date"] = pd.to_datetime(expected_data["date"])
    symbols = ["PLACE_HOLDER"]

    monkeypatch.setattr(
        ph,
        "get_daily_prices",
        lambda *args: pd.read_csv(
            TEST_DATA_DIR.joinpath(f"raw_{asset_category}_prices.csv"),
            header=[0, 1],
            index_col=[0],
            parse_dates=True,
        ),
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
