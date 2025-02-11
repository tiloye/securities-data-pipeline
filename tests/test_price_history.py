from pathlib import Path

import pandas as pd
import pytest

import pipeline.price_history as ph


TEST_DATA_DIR = Path(__file__).parent.joinpath("data")


@pytest.mark.parametrize(
    ("asset_category", "symbols"),
    (
        ("fx", ["INVALID_FX_SYMBOL_1", "INVALID_FX_SYMBOL_2"]),
        ("sp_stocks", ["INVALID_STOCK_SYMBOL_1", "INVALID_STOCK_SYMBOL_2"]),
    ),
)
def test_check_error_tracks_failed_downloads(monkeypatch, asset_category, symbols):
    monkeypatch.setattr(
        ph.yf,
        "download",
        lambda symbols, start, end, group_by, auto_adjust: None,
    )  # Avoid sending request to Yahoo Finance
    monkeypatch.setitem(ph.yf.shared._ERRORS, symbols[0], "Error message")
    monkeypatch.setitem(ph.yf.shared._ERRORS, symbols[1], "Error message")

    data = ph.get_daily_prices(symbols)
    ph.check_errors(asset_category)

    assert ph.YF_ERRORS[asset_category] == list(ph.yf.shared._ERRORS.keys())


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_transform_returns_None_if_df_is_empty(asset_category):
    df = pd.DataFrame(columns=["Ticker", "Open", "High", "Low", "Close"])

    transformed_df = ph.transform(df, asset_category)

    assert transformed_df is None


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_transform_returns_expected_df(asset_category):
    ph.YF_ERRORS[asset_category].extend(["INVALID_SYMBOL_1", "INVALID_SYMBOL_2"])
    price_data = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"raw_{asset_category}_prices.csv"),
        header=[0, 1],
        index_col=[0],
        parse_dates=True,
    )
    expected_df = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv"),
        parse_dates=["date"],
    )
    expected_df["date"] = expected_df["date"].dt.date

    transformed_price_data = ph.transform(price_data, asset_category)

    pd.testing.assert_frame_equal(transformed_price_data, expected_df)
