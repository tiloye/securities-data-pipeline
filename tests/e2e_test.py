from pathlib import Path

import pandas as pd
import pytest
from deltalake import DeltaTable
from prefect.testing.utilities import prefect_test_harness
from sqlalchemy import create_engine

from py_pipeline.config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    DATA_PATH,
    S3_ENDPOINT,
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_PASSWORD,
    DB_NAME,
)
from py_pipeline.extract import YF_ERRORS
from py_pipeline.orchestration import (
    etl_price_history_source_to_s3,
    etl_symbols_source_to_s3,
    el_symbols_s3_to_dw,
    el_price_history_s3_to_dw,
)

FX_SYMBOLS = [
    "EURUSD=X",
    "GBPUSD=X",
    "INVALID_SYMBOL_1",
    "AUDUSD=X",
    "NZDUSD=X",
    "JPY=X",
    "CHF=X",
    "CAD=X",
    "INVALID_SYMBOL_2",
]
SP_SYMBOLS = ["AAPL", "INVALID_SYMBOL_1", "MSFT", "BRK-A", "BRK-B", "INVALID_SYMBOL_2"]

TEST_DATA_DIR = Path(__file__).parent.joinpath("data")
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

s3_storage_options = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_ENDPOINT_URL": S3_ENDPOINT,
    "AWS_ALLOW_HTTP": "true",
}


def assert_loaded_data_matches_expected(loaded_df, expected_df):
    assert loaded_df.shape == expected_df.shape
    assert sorted(loaded_df.columns.tolist()) == sorted(expected_df.columns.tolist())


@pytest.fixture(autouse=True, scope="module")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


########## Tests for Symbols ETL ##############


def test_s3_etl_fx_symbols(remove_s3_objects):
    expected_data = (
        pd.read_parquet(TEST_DATA_DIR.joinpath("processed_fx_symbols.parquet"))
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    etl_symbols_source_to_s3("fx")
    loaded_data = (
        DeltaTable(f"{DATA_PATH}/symbols/fx", storage_options=s3_storage_options)
        .to_pandas()
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(loaded_data, expected_data)


def test_s3_etl_sp_symbols(monkeypatch, remove_s3_objects):
    symbols_df = pd.read_csv(TEST_DATA_DIR.joinpath("raw_sp_stocks_symbols.csv"))
    monkeypatch.setattr(
        "py_pipeline.extract.get_sp_stock_symbols_from_source", lambda: symbols_df
    )

    etl_symbols_source_to_s3("sp_stocks", date_stamp=pd.Timestamp("2000-01-03").date())
    loaded_data = (
        DeltaTable(f"{DATA_PATH}/symbols/sp_stocks", storage_options=s3_storage_options)
        .to_pandas()
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    expected_data = (
        pd.read_parquet(
            TEST_DATA_DIR.joinpath("processed_sp_stocks_symbols.parquet"),
            filters=[("date_stamp", "=", pd.Timestamp("2000-01-03").date())],
        )
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    assert_loaded_data_matches_expected(loaded_data, expected_data)


def test_dw_el_fx_symbols(drop_dw_tables, remove_s3_objects):
    etl_symbols_source_to_s3("fx")

    el_symbols_s3_to_dw("fx")

    loaded_data = (
        pd.read_sql_table("symbols_fx", con=engine)
        .sort_values("symbol")
        .reset_index(drop=True)
    )
    expected_data = (
        pd.read_parquet(TEST_DATA_DIR.joinpath("processed_fx_symbols.parquet"))
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    assert_loaded_data_matches_expected(loaded_data, expected_data)


def test_dw_el_sp_stocks_symbols(monkeypatch, remove_s3_objects, drop_dw_tables):
    symbols_df = pd.read_csv(TEST_DATA_DIR.joinpath("raw_sp_stocks_symbols.csv"))
    monkeypatch.setattr(
        "py_pipeline.extract.get_sp_stock_symbols_from_source", lambda: symbols_df
    )
    etl_symbols_source_to_s3("sp_stocks", date_stamp=pd.Timestamp("2000-01-03").date())

    el_symbols_s3_to_dw("sp_stocks")

    loaded_data = (
        pd.read_sql_table("symbols_sp_stocks", con=engine)
        .sort_values("symbol")
        .reset_index(drop=True)
    )
    expected_data = (
        pd.read_parquet(
            TEST_DATA_DIR.joinpath("processed_sp_stocks_symbols.parquet"),
            filters=[("date_stamp", "=", pd.Timestamp("2000-01-03").date())],
        )
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    assert_loaded_data_matches_expected(loaded_data, expected_data)


########## Tests for Price History ETL ##############
@pytest.fixture
def price_data():
    def _price_data(
        asset_category, symbols=None, start=None, end=None, drop_invalid=True
    ):
        data = pd.read_csv(
            TEST_DATA_DIR.joinpath(f"raw_{asset_category}_prices.csv"),
            header=[0, 1],
            index_col=[0],
            parse_dates=True,
        )
        if drop_invalid:
            data = data.drop(
                columns=pd.MultiIndex.from_product(
                    (
                        ["Open", "High", "Low", "Close", "Adj Close", "Volume"],
                        ["INVALID_SYMBOL_1", "INVALID_SYMBOL_2"],
                    )
                )
            )
        if start and end:
            data = data.loc[start:end]
        return data.loc[:, pd.IndexSlice[:, symbols]] if symbols else data

    return _price_data


@pytest.mark.usefixtures("remove_s3_objects")
class TestETLBars:
    def _etl_bars_to_s3(
        self,
        monkeypatch,
        price_data,
        asset_category,
        start=None,
        end=None,
        drop_invalid=True,
    ):
        symbols = FX_SYMBOLS if asset_category == "fx" else SP_SYMBOLS

        monkeypatch.setattr(
            "py_pipeline.extract.yf.download",
            lambda *args, **kwargs: price_data(
                asset_category, start=start, end=end, drop_invalid=drop_invalid
            ),
        )
        if not drop_invalid:
            monkeypatch.setitem(
                YF_ERRORS, asset_category, ["INVALID_SYMBOL_1", "INVALID_SYMBOL_2"]
            )

        etl_price_history_source_to_s3(
            asset_category, symbols, start, end, chunk_size=500
        )

    @pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
    def test_s3_etl_bars(self, monkeypatch, price_data, asset_category, subtests):
        with subtests.test(msg="ETL raises error when there are unknown symbols"):
            with pytest.raises(RuntimeError):
                self._etl_bars_to_s3(
                    monkeypatch, price_data, asset_category, drop_invalid=False
                )

        with subtests.test(msg="ETL loads known symbols to S3"):
            expected_data = pd.read_parquet(
                TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.parquet")
            )

            loaded_data = DeltaTable(
                f"{DATA_PATH}/price_history/{asset_category}",
                storage_options=s3_storage_options,
            ).to_pandas()

            assert_loaded_data_matches_expected(loaded_data, expected_data)

    @pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
    def test_dw_el_bars(self, monkeypatch, price_data, asset_category, drop_dw_tables):
        self._etl_bars_to_s3(monkeypatch, price_data, asset_category)

        el_price_history_s3_to_dw(asset_category, start_date=None, end_date=None)

        loaded_data = pd.read_sql_table(f"price_history_{asset_category}", con=engine)
        expected_data = pd.read_parquet(
            TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.parquet"),
        )

        assert_loaded_data_matches_expected(loaded_data, expected_data)

    @pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
    def test_s3_dw_etl_update_existing_data(
        self, monkeypatch, price_data, asset_category, drop_dw_tables
    ):
        # First ETL run
        start = "2000-01-03"
        end = "2000-01-06"

        self._etl_bars_to_s3(
            monkeypatch,
            price_data,
            asset_category,
            start=start,
            end=end,
        )
        el_price_history_s3_to_dw(asset_category, start_date=start, end_date=end)

        # Second ETL run to update data
        start = "2000-01-07"
        end = "2000-01-07"

        self._etl_bars_to_s3(
            monkeypatch,
            price_data,
            asset_category,
            start=start,
            end=end,
        )
        el_price_history_s3_to_dw(asset_category, start_date=start, end_date=end)

        loaded_s3_data = DeltaTable(
            f"{DATA_PATH}/price_history/{asset_category}",
            storage_options=s3_storage_options,
        ).to_pandas()
        expected_s3_data = pd.read_parquet(
            TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.parquet"),
        )

        assert_loaded_data_matches_expected(loaded_s3_data, expected_s3_data)

        loaded_dw_data = pd.read_sql_table(
            f"price_history_{asset_category}", con=engine
        )
        expected_dw_data = expected_s3_data.copy()

        assert_loaded_data_matches_expected(loaded_dw_data, expected_dw_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_s3_etl_bars_in_chunk(
    monkeypatch, price_data, asset_category, subtests, remove_s3_objects
):
    monkeypatch.setattr(
        "py_pipeline.extract.yf.download",
        lambda *args, **kwargs: price_data(asset_category, *args, drop_invalid=False),
    )
    monkeypatch.setitem(
        YF_ERRORS,
        asset_category,
        ["INVALID_SYMBOL_1", "INVALID_SYMBOL_2"],
    )

    symbols = FX_SYMBOLS if asset_category == "fx" else SP_SYMBOLS
    with subtests.test(msg="ETL in chunks raises error when there are unknown symbols"):
        with pytest.raises(RuntimeError):
            etl_price_history_source_to_s3(
                asset_category, symbols, "2000-01-01", "2000-01-05", chunk_size=2
            )

    with subtests.test(
        msg="Check that loading ETL in chunks loads known symbols to S3"
    ):
        loaded_data = (
            DeltaTable(
                f"{DATA_PATH}/price_history/{asset_category}",
                storage_options=s3_storage_options,
            )
            .to_pandas()
            .sort_values(["date_stamp", "symbol"])
            .reset_index(drop=True)
        )
        expected_data = pd.read_parquet(
            TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.parquet"),
        )
        expected_data.sort_values(["date_stamp", "symbol"], inplace=True)
        expected_data.reset_index(drop=True, inplace=True)

        assert_loaded_data_matches_expected(loaded_data, expected_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_s3_etl_bars_raises_exception(monkeypatch, asset_category):
    """
    Tests whether etl_bars function raises exception when tranformation function
    returns emtpy data frame.
    """

    symbols = ["PLACE_HOLDER"]
    monkeypatch.setattr(
        "py_pipeline.extract.yf.download", lambda *args, **kwargs: pd.DataFrame()
    )
    monkeypatch.setitem(YF_ERRORS, asset_category, symbols)

    with pytest.raises(RuntimeError):
        etl_price_history_source_to_s3(
            asset_category, symbols, "2000-01-01", "2000-01-05", chunk_size=500
        )


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

    with pytest.raises(RuntimeError):
        etl_price_history_source_to_s3(
            asset_category, symbols, "2000-01-01", "2000-01-05", chunk_size=2
        )


if __name__ == "__main__":
    pytest.main([__file__])
