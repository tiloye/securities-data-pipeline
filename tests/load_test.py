from pathlib import Path

import pandas as pd
import pandera.pandas as pa
import pytest
from deltalake import DeltaTable
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
from py_pipeline.load import load_to_dw, load_to_s3

TEST_DATA_DIR = Path(__file__).parent.joinpath("data")
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

storage_options = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_ENDPOINT_URL": S3_ENDPOINT,
    "AWS_ALLOW_HTTP": "true",
}


def assert_loaded_data_matches_expected(loaded_df, expected_df):
    assert loaded_df.shape == expected_df.shape
    assert sorted(loaded_df.columns.tolist()) == sorted(expected_df.columns.tolist())


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_symbols_data_to_s3_raises_schema_error(asset_category):
    source_symbol = pd.read_parquet(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.parquet")
    )
    source_symbol.rename(columns={"symbol": "ticker"}, inplace=True)

    with pytest.raises(pa.errors.SchemaErrors):
        load_to_s3(source_symbol, "symbols", asset_category)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_symbols_data_to_s3(asset_category, remove_s3_objects):
    symbols = pd.read_parquet(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.parquet")
    )

    load_to_s3(symbols, "symbols", asset_category)

    loaded_symbols = DeltaTable(
        f"{DATA_PATH}/symbols/{asset_category}", storage_options=storage_options
    ).to_pandas()

    assert_loaded_data_matches_expected(loaded_symbols, symbols)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_symbols_data_to_dw(asset_category, drop_dw_tables):
    symbols = (
        pd.read_parquet(
            TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.parquet")
        )
        .sort_values(
            ["symbol", "date_stamp"] if asset_category == "sp_stocks" else "symbol"
        )
        .reset_index(drop=True)
    )

    load_to_dw(symbols, "symbols", asset_category)

    loaded_data = (
        pd.read_sql_table(f"symbols_{asset_category}", con=engine)
        .sort_values(
            ["symbol", "date_stamp"] if asset_category == "sp_stocks" else "symbol"
        )
        .reset_index(drop=True)
    )

    assert_loaded_data_matches_expected(loaded_data, symbols)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_update_symbols_data_on_s3(asset_category, remove_s3_objects):
    # Load historical data
    symbols = pd.read_parquet(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.parquet")
    )
    load_to_s3(symbols, "symbols", asset_category)

    # Load update
    if asset_category == "sp_stocks":
        symbols_update = pd.read_parquet(
            TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols_update.parquet")
        )
    else:
        symbols_update = symbols.copy()

    load_to_s3(symbols_update, "symbols", asset_category)

    # Verify
    expected_data = (
        pd.concat([symbols, symbols_update])
        .sort_values(["symbol", "date_stamp"])
        .reset_index(drop=True)
        if asset_category == "sp_stocks"
        else symbols.sort_values("symbol").reset_index(drop=True)
    )

    loaded_symbols = DeltaTable(
        f"{DATA_PATH}/symbols/{asset_category}", storage_options=storage_options
    ).to_pandas()

    assert_loaded_data_matches_expected(loaded_symbols, expected_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_update_symbols_data_on_dw(asset_category, drop_dw_tables):
    # load historical data
    symbols = pd.read_parquet(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.parquet")
    )
    load_to_dw(symbols, "symbols", asset_category)

    # Load updates
    if asset_category == "sp_stocks":
        symbols_update = pd.read_parquet(
            TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols_update.parquet")
        )
    else:
        symbols_update = symbols.copy()
    load_to_dw(symbols_update, "symbols", asset_category)

    # Verify
    expected_data = (
        pd.concat([symbols, symbols_update])
        .sort_values(["symbol", "date_stamp"])
        .reset_index(drop=True)
        if asset_category == "sp_stocks"
        else symbols.sort_values("symbol").reset_index(drop=True)
    )
    loaded_data = pd.read_sql_table(f"symbols_{asset_category}", con=engine)
    loaded_data = (
        loaded_data.sort_values(["symbol", "date_stamp"]).reset_index(drop=True)
        if asset_category == "sp_stocks"
        else loaded_data.sort_values("symbol").reset_index(drop=True)
    )

    assert_loaded_data_matches_expected(loaded_data, expected_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_price_data_to_s3_raises_schema_error(asset_category):
    transformed_prices = pd.read_parquet(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.parquet")
    )
    transformed_prices.rename(columns={"date_stamp": "timestamp"}, inplace=True)

    with pytest.raises(pa.errors.SchemaErrors):
        load_to_s3(transformed_prices, "price_history", asset_category)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_price_data_to_s3(asset_category, remove_s3_objects):
    price_df = pd.read_parquet(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.parquet"),
    )

    load_to_s3(price_df, "price_history", asset_category)

    loaded_price_df = DeltaTable(
        f"{DATA_PATH}/price_history/{asset_category}", storage_options=storage_options
    ).to_pandas()

    assert_loaded_data_matches_expected(loaded_price_df, price_df)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_price_data_to_dw(asset_category, drop_dw_tables):
    price_df = (
        pd.read_parquet(
            TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.parquet")
        )
        .sort_values(["date_stamp", "symbol"])
        .reset_index(drop=True)
    )

    load_to_dw(price_df, "price_history", asset_category)

    loaded_data = (
        pd.read_sql_table(f"price_history_{asset_category}", con=engine)
        .sort_values(["date_stamp", "symbol"])
        .reset_index(drop=True)
    )

    assert_loaded_data_matches_expected(loaded_data, price_df)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_update_price_on_s3(asset_category, remove_s3_objects):
    # Load historical price
    hist_price_df = pd.read_parquet(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.parquet")
    )
    load_to_s3(hist_price_df, "price_history", asset_category)

    # Load price update
    price_update = pd.read_parquet(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices_update.parquet")
    )
    load_to_s3(price_update, "price_history", asset_category)

    # Verify merged data
    expected_df = (
        pd.concat([hist_price_df, price_update], ignore_index=True)
        .sort_values(["date_stamp", "symbol"])
        .reset_index(drop=True)
    )

    loaded_price_df = DeltaTable(
        f"{DATA_PATH}/price_history/{asset_category}", storage_options=storage_options
    ).to_pandas()

    assert_loaded_data_matches_expected(loaded_price_df, expected_df)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_update_price_on_dw(asset_category, drop_dw_tables):
    # Load historical price
    hist_price_df = pd.read_parquet(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.parquet")
    )
    load_to_dw(hist_price_df, "price_history", asset_category)

    # Load price update with existing records
    price_update = pd.read_parquet(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices_update.parquet")
    )
    price_update_with_existing = pd.concat(
        [hist_price_df, price_update], ignore_index=True
    )
    load_to_dw(price_update_with_existing, "price_history", asset_category)

    # Verify merged data
    expected_df = (
        pd.concat([hist_price_df, price_update], ignore_index=True)
        .sort_values(["date_stamp", "symbol"])
        .reset_index(drop=True)
    )

    loaded_price_df = (
        pd.read_sql_table(f"price_history_{asset_category}", con=engine)
        .sort_values(["date_stamp", "symbol"])
        .reset_index(drop=True)
    )

    assert_loaded_data_matches_expected(loaded_price_df, expected_df)


if __name__ == "__main__":
    pytest.main([__file__])
