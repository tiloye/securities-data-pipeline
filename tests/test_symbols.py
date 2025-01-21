from pathlib import Path

import pandas as pd
import pytest

from pipeline.symbols import get_sp_stock_symbols, transform_stocks_df


TEST_DATA_DIR = Path(__file__).parent.joinpath("data")


@pytest.fixture
def expected_sp_symbols_data():
    symbols_df = pd.read_csv(TEST_DATA_DIR.joinpath("raw_sp_symbols.csv"))
    return symbols_df


@pytest.fixture
def unexpected_sp_symbols_data(expected_sp_symbols_data):
    return expected_sp_symbols_data.rename(columns={"Symbol": "Ticker"})


def test_get_stock_symbols_returns_None(monkeypatch, unexpected_sp_symbols_data):
    """Check that None is returned when dataframe does not have expected column."""

    monkeypatch.setattr(
        pd, "read_html", lambda url: [None]
    )  # Do not send request to wiki page
    monkeypatch.setattr(pd, "concat", lambda objs: unexpected_sp_symbols_data)

    data = get_sp_stock_symbols.fn()

    assert data is None


def test_transform_stocks_df(expected_sp_symbols_data):
    expected_transformed_data = pd.read_csv(
        TEST_DATA_DIR.joinpath("processed_sp_stocks_symbols.csv")
    )

    transformed_data = transform_stocks_df.fn(expected_sp_symbols_data)

    # Sort dataframe by symbol to ensure it has the same order with expected dataframe
    transformed_data = transformed_data.sort_values("symbol").reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_data, expected_transformed_data)
