from pathlib import Path

import pandas as pd
import pytest

from pipeline.symbols import transform_stocks_df


TEST_DATA_DIR = Path(__file__).parent.joinpath("data")


@pytest.fixture
def expected_sp_symbols_data():
    symbols_df = pd.read_csv(TEST_DATA_DIR.joinpath("raw_sp_symbols.csv"))
    return symbols_df


def test_transform_stocks_df(expected_sp_symbols_data):
    expected_transformed_data = pd.read_csv(
        TEST_DATA_DIR.joinpath("processed_sp_stocks_symbols.csv")
    )

    transformed_data = transform_stocks_df.fn(expected_sp_symbols_data)

    # Sort dataframe by symbol to ensure it has the same order with expected dataframe
    transformed_data = transformed_data.sort_values("symbol").reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_data, expected_transformed_data)
