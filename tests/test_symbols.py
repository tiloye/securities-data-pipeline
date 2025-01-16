import pandas as pd
import pytest

from pipeline.symbols import get_sp_stock_symbols, transform_stocks_df


@pytest.fixture
def expected_symbols_data():
    sp600_data = pd.DataFrame(
        data=[
            ["A", "Stock A", "Sector 1", "Industry 1", "Location 1", "view", 1],
            ["B", "Stock B", "Sector 2", "Industry 2", "Location 2", "view", 2],
        ],
        columns=[
            "Symbol",
            "Company",
            "GICS Sector",
            "GICS Sub-Industry",
            "Headquarters Location",
            "SEC filings",
            "CIK",
        ],
    )

    sp500_data = pd.DataFrame(
        data=[
            [
                "C",
                "Stock C",
                "Sector 1",
                "Industry 1",
                "Location 1",
                "1990-01-01",
                3,
                "1908",
            ],
            [
                "D",
                "Stock D",
                "Sector 2",
                "Industry 2",
                "Location 2",
                "1991-01-01",
                4,
                "1904",
            ],
        ],
        columns=[
            "Symbol",
            "Security",
            "GICS Sector",
            "GICS Sub-Industry",
            "Headquarters Location",
            "Date Added",
            "CIK",
            "Founded",
        ],
    )

    sp400_data = pd.DataFrame(
        data=[
            ["E", "Stock E", "Sector 1", "Industry 1", "Location 1", "reports"],
            ["F", "Stock F", "Sector 2", "Industry 2", "Location 2", "reports"],
        ],
        columns=[
            "Symbol",
            "Security",
            "GICS Sector",
            "GICS Sub-Industry",
            "Headquarters Location",
            "SEC filings",
        ],
    )

    symbols_df = pd.concat([sp400_data, sp500_data, sp600_data])

    return symbols_df


@pytest.fixture
def unexpected_sp_symbols_data(expected_symbols_data):
    return expected_symbols_data.rename(columns={"Symbol": "Ticker"})


def test_get_stock_symbols_returns_dataframe(monkeypatch, expected_symbols_data):
    """Check that the expected dataframe is returned"""

    monkeypatch.setattr(
        pd, "read_html", lambda url: [None]
    )  # Do not send request to wiki page
    monkeypatch.setattr(pd, "concat", lambda objs: expected_symbols_data)
    data = get_sp_stock_symbols.fn()

    pd.testing.assert_frame_equal(data, expected_symbols_data)


def test_get_stock_symbols_returns_None(monkeypatch, unexpected_sp_symbols_data):
    """Check that None is returned when dataframe does not have expected column."""

    monkeypatch.setattr(
        pd, "read_html", lambda url: [None]
    )  # Do not send request to wiki page
    monkeypatch.setattr(pd, "concat", lambda objs: unexpected_sp_symbols_data)

    data = get_sp_stock_symbols.fn()

    assert data is None


def test_transform_stocks_df(expected_symbols_data):
    expected_transformed_data = pd.DataFrame(
        data=[
            ("A", "Stock A", "Sector 1", "Industry 1"),
            ("B", "Stock B", "Sector 2", "Industry 2"),
            ("C", "Stock C", "Sector 1", "Industry 1"),
            ("D", "Stock D", "Sector 2", "Industry 2"),
            ("E", "Stock E", "Sector 1", "Industry 1"),
            ("F", "Stock F", "Sector 2", "Industry 2"),
        ],
        columns=["symbol", "name", "sector", "industry"],
    )

    transformed_data = transform_stocks_df.fn(expected_symbols_data)

    # Sort dataframe by symbol to ensure it in the same order with expected dataframe
    transformed_data = transformed_data.sort_values("symbol").reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_data, expected_transformed_data)
