from pathlib import Path

import pandas as pd
import pytest
from minio import Minio
from prefect.testing.utilities import prefect_test_harness

from pipeline import symbols, s3_el


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
def sp_symbols():
    symbols_df = pd.read_csv(TEST_DATA_DIR.joinpath("raw_sp_symbols.csv"))
    return symbols_df


def test_etl_fx_symbols():
    expected_data = (
        pd.read_csv(TEST_DATA_DIR.joinpath("raw_fx_symbols.csv"))
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    symbols.etl_fx_symbols()
    loaded_data = (
        pd.read_csv(
            f"s3://{s3_el.BUCKET_NAME}/symbols/fx.csv",
            storage_options=s3_el.s3_storage_options,
        )
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(loaded_data, expected_data)


def test_etl_sp_symbols(monkeypatch, sp_symbols):
    expected_data = (
        pd.read_csv(TEST_DATA_DIR.joinpath("processed_sp_stocks_symbols.csv"))
        .sort_values("symbol")
        .reset_index(drop=True)
    )
    monkeypatch.setattr(symbols, "get_sp_stock_symbols", lambda: sp_symbols)

    symbols.etl_sp_stocks_symbols()
    loaded_data = (
        pd.read_csv(
            f"s3://{s3_el.BUCKET_NAME}/symbols/sp_stocks.csv",
            storage_options=s3_el.s3_storage_options,
        )
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(loaded_data, expected_data)
