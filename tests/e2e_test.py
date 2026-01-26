from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.engine import create_engine

from py_pipeline.config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    BUCKET_NAME,
    DATA_PATH,
    S3_ENDPOINT,
    DB_ENGINE,
)
from py_pipeline.extract import YF_ERRORS
from py_pipeline.main import (
    etl_bars_to_s3,
    etl_fx_symbols_to_s3,
    etl_sp_stocks_symbols_to_s3,
    el_symbols_to_dw,
    el_bars_to_dw,
)

FX_SYMBOLS = ["EURUSD=X", "GBPUSD=X", "AUDUSD=X", "NZDUSD=X", "JPY=X", "CHF=X", "CAD=X"]
SP_SYMBOLS = ["AAPL", "INVALID_SYMBOL_1", "MSFT", "BRK-A", "BRK-B", "INVALID_SYMBOL_2"]

TEST_DATA_DIR = Path(__file__).parent.joinpath("data")
engine = DB_ENGINE

s3_storage_options = {
    "key": AWS_ACCESS_KEY,
    "secret": AWS_SECRET_KEY,
    "endpoint_url": S3_ENDPOINT,
}


########## Tests for Symbols ETL ##############


def test_s3_etl_fx_symbols(remove_s3_objects):
    expected_data = (
        pd.read_csv(TEST_DATA_DIR.joinpath("processed_fx_symbols.csv"))
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    etl_fx_symbols_to_s3()
    loaded_data = (
        pd.read_csv(
            f"{DATA_PATH}/symbols/fx.csv",
            storage_options=s3_storage_options,
        )
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(loaded_data, expected_data)


def test_s3_etl_sp_symbols(monkeypatch, remove_s3_objects):
    symbols_df = pd.read_csv(TEST_DATA_DIR.joinpath("raw_sp_symbols.csv"))
    monkeypatch.setattr(
        "py_pipeline.main.get_sp_stock_symbols_from_source", lambda: symbols_df
    )

    etl_sp_stocks_symbols_to_s3()
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


def test_dw_el_fx_symbols():
    etl_fx_symbols_to_s3()

    el_symbols_to_dw("fx")

    loaded_data = (
        pd.read_sql_table("symbols_fx", con=engine)
        .sort_values("symbol")
        .reset_index(drop=True)
    )
    expected_data = (
        pd.read_csv(TEST_DATA_DIR.joinpath("processed_fx_symbols.csv"))
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(expected_data, loaded_data)

    with engine.connect() as con:
        con.execute(text("DROP TABLE symbols_fx;"))
        con.commit()


def test_dw_el_sp_stocks_symbols(monkeypatch):
    symbols_df = pd.read_csv(TEST_DATA_DIR.joinpath("raw_sp_symbols.csv"))
    monkeypatch.setattr(
        "py_pipeline.main.get_sp_stock_symbols_from_source", lambda: symbols_df
    )
    etl_sp_stocks_symbols_to_s3()

    el_symbols_to_dw("sp_stocks")

    loaded_data = (
        pd.read_sql_table("symbols_sp_stocks", con=engine)
        .sort_values("symbol")
        .reset_index(drop=True)
    )
    expected_data = (
        pd.read_csv(TEST_DATA_DIR.joinpath("processed_sp_stocks_symbols.csv"))
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(expected_data, loaded_data)

    with engine.connect() as con:
        con.execute(text("DROP TABLE symbols_sp_stocks;"))
        con.commit()


########## Tests for Price History ETL ##############
@pytest.fixture
def price_data():
    def _price_data(asset_category, symbols=None, start=None, end=None):
        data = pd.read_csv(
            TEST_DATA_DIR.joinpath(f"raw_{asset_category}_prices.csv"),
            header=[0, 1],
            index_col=[0],
            parse_dates=True,
        )
        if start and end:
            data = data.loc[start:end]
        return data[symbols] if symbols else data

    return _price_data


@pytest.mark.usefixtures("remove_s3_objects")
class TestETLBars:
    def _etl_bars_to_s3(
        self, monkeypatch, price_data, asset_category, start=None, end=None
    ):
        symbols = FX_SYMBOLS if asset_category == "fx" else SP_SYMBOLS

        monkeypatch.setattr(
            "py_pipeline.extract.yf.download",
            lambda *args, **kwargs: price_data(asset_category, start=start, end=end),
        )
        if asset_category == "sp_stocks":
            monkeypatch.setitem(
                YF_ERRORS, "sp_stocks", ["INVALID_SYMBOL_1", "INVALID_SYMBOL_2"]
            )

        etl_bars_to_s3(asset_category, symbols, start, end)

    @pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
    def test_s3_etl_bars(self, monkeypatch, price_data, asset_category):
        self._etl_bars_to_s3(monkeypatch, price_data, asset_category)

        expected_data = pd.read_csv(
            TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv")
        )
        expected_data["date"] = pd.to_datetime(expected_data["date"]).astype(
            "datetime64[us]"
        )
        loaded_data = pd.read_parquet(
            f"{DATA_PATH}/price_history/{asset_category}.parquet",
            storage_options=s3_storage_options,
        )

        pd.testing.assert_frame_equal(loaded_data, expected_data)

    @pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
    def test_dw_el_bars(self, monkeypatch, price_data, asset_category, drop_dw_tables):
        self._etl_bars_to_s3(monkeypatch, price_data, asset_category)

        el_bars_to_dw(asset_category)

        loaded_data = pd.read_sql_table(f"price_history_{asset_category}", con=engine)
        expected_data = pd.read_csv(
            TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv"),
        )
        expected_data["date"] = pd.to_datetime(expected_data["date"])

        pd.testing.assert_frame_equal(
            loaded_data.sort_values(["date", "symbol"]).reset_index(drop=True),
            expected_data.sort_values(["date", "symbol"]).reset_index(drop=True),
        )

    @pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
    def test_s3_dw_etl_update_existing_data(
        self, monkeypatch, price_data, asset_category, drop_dw_tables
    ):
        # First ETL run
        if asset_category == "sp_stocks":
            start = "2000-01-03"
            end = "2000-01-06"
        else:
            start = "2006-05-16"
            end = "2006-05-19"

        self._etl_bars_to_s3(
            monkeypatch,
            price_data,
            asset_category,
            start=start,
            end=end,
        )
        el_bars_to_dw(asset_category)

        # Second ETL run to update data
        if asset_category == "sp_stocks":
            start = "2000-01-07"
            end = "2000-01-07"
        else:
            start = "2006-05-22"
            end = "2006-05-22"

        self._etl_bars_to_s3(
            monkeypatch,
            price_data,
            asset_category,
            start=start,
            end=end,
        )
        el_bars_to_dw(asset_category, start, end)

        loaded_s3_data = pd.read_parquet(
            f"{DATA_PATH}/price_history/{asset_category}.parquet",
            storage_options=s3_storage_options,
        )
        expected_s3_data = pd.read_csv(
            TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv"),
        )
        expected_s3_data["date"] = pd.to_datetime(expected_s3_data["date"]).astype(
            "datetime64[us]"
        )

        pd.testing.assert_frame_equal(
            loaded_s3_data.sort_values(["date", "symbol"]).reset_index(drop=True),
            expected_s3_data.sort_values(["date", "symbol"]).reset_index(drop=True),
        )

        loaded_dw_data = pd.read_sql_table(
            f"price_history_{asset_category}", con=engine
        )
        loaded_dw_data["date"] = pd.to_datetime(loaded_dw_data["date"]).astype(
            "datetime64[us]"
        )
        expected_dw_data = expected_s3_data.copy()

        pd.testing.assert_frame_equal(
            loaded_dw_data.sort_values(["date", "symbol"]).reset_index(drop=True),
            expected_dw_data.sort_values(["date", "symbol"]).reset_index(drop=True),
        )


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_s3_etl_bars_in_chunk(
    monkeypatch, price_data, asset_category, remove_s3_objects
):
    monkeypatch.setattr(
        "py_pipeline.extract.yf.download",
        lambda *args, **kwargs: price_data(asset_category, args[0]),
    )
    if asset_category == "sp_stocks":
        monkeypatch.setitem(
            YF_ERRORS, "sp_stocks", ["INVALID_SYMBOL_1", "INVALID_SYMBOL_2"]
        )

    symbols = FX_SYMBOLS if asset_category == "fx" else SP_SYMBOLS
    etl_bars_to_s3(asset_category, symbols, "2000-01-01", "2000-01-05", chunk_size=2)

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
    )
    expected_data["date"] = pd.to_datetime(expected_data["date"]).astype(
        "datetime64[us]"
    )
    expected_data.sort_values(["date", "symbol"], inplace=True)
    expected_data.reset_index(drop=True, inplace=True)

    pd.testing.assert_frame_equal(loaded_data, expected_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_s3_etl_bars_raises_exception(monkeypatch, asset_category):
    """
    Tests whether etl_bars function raises exception when tranformation function
    returns emtpy data frame.
    """

    monkeypatch.setattr(
        "py_pipeline.extract.yf.download", lambda *args, **kwargs: pd.DataFrame()
    )

    with pytest.raises(ValueError):
        symbols = ["PLACE_HOLDER"]
        etl_bars_to_s3(asset_category, symbols, "2000-01-01", "2000-01-05")


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_s3_etl_bars_in_chunk_raises_exception(monkeypatch, asset_category):
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
        "py_pipeline.extract.yf.download", lambda *args, **kwargs: pd.DataFrame()
    )
    monkeypatch.setitem(YF_ERRORS, asset_category, symbols)

    with pytest.raises(ValueError):
        etl_bars_to_s3(
            asset_category, symbols, "2000-01-01", "2000-01-05", chunk_size=2
        )


if __name__ == "__main__":
    pytest.main([__file__])
