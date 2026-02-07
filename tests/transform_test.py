from pathlib import Path

import pandas as pd
import pytest

from py_pipeline.transform import (
    transform_stocks_symbol_df,
    transform_fx_symbol_df,
    transform_price_df,
)

TEST_DATA_DIR = Path(__file__).parent.joinpath("data")


def test_transform_stocks_symbol_df(monkeypatch):
    expected_transformed_data = pd.read_csv(
        TEST_DATA_DIR.joinpath("processed_sp_stocks_symbols.csv")
    ).sort_values("symbol").reset_index(drop=True)
    monkeypatch.setattr("py_pipeline.transform.date_stamp", lambda: "2026-01-01")

    symbols_df = pd.read_csv(TEST_DATA_DIR.joinpath("raw_sp_symbols.csv"))
    transformed_data = transform_stocks_symbol_df(symbols_df)

    # Sort dataframe by symbol to ensure it has the same order with expected dataframe
    transformed_data = transformed_data.sort_values("symbol").reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_data, expected_transformed_data)


def test_transform_fx_symbol_df():
    expected_transformed_data = pd.read_csv(
        TEST_DATA_DIR.joinpath("processed_fx_symbols.csv")
    )

    symbols_df = pd.read_csv(TEST_DATA_DIR.joinpath("raw_fx_symbols.csv"))
    transformed_data = transform_fx_symbol_df(symbols_df)

    # Sort dataframe by symbol to ensure it has the same order with expected dataframe
    transformed_data = transformed_data.reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_data, expected_transformed_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_transform_price_df_returns_empty_df(asset_category):
    df = pd.DataFrame(columns=["Ticker", "Open", "High", "Low", "Close"])

    transformed_df = transform_price_df(df, asset_category)

    assert transformed_df.empty


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_transform_price_df_returns_expected_df(asset_category):
    price_data = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"raw_{asset_category}_prices.csv"),
        header=[0, 1],
        index_col=[0],
        parse_dates=True,
    )
    expected_df = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv"),
    )
    expected_df["date"] = pd.to_datetime(expected_df["date"]).astype("datetime64[us]")

    transformed_price_data = transform_price_df(price_data, asset_category)

    pd.testing.assert_frame_equal(transformed_price_data, expected_df)


if __name__ == "__main__":
    pytest.main([__file__])
