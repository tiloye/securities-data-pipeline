from pathlib import Path

import pandas as pd
import pytest

from py_pipeline.config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    DB_ENGINE,
    DATA_PATH,
    S3_ENDPOINT,
)
from py_pipeline.load import load_to_dw, load_to_s3

TEST_DATA_DIR = Path(__file__).parent.joinpath("data")
engine = DB_ENGINE


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_symbols_data_to_s3(asset_category, remove_s3_objects):
    symbols = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.csv")
    )

    load_to_s3(symbols, "symbols", asset_category)

    loaded_symbols = pd.read_parquet(
        f"{DATA_PATH}/symbols/{asset_category}.parquet",
        storage_options={
            "key": AWS_ACCESS_KEY,
            "secret": AWS_SECRET_KEY,
            "endpoint_url": S3_ENDPOINT,
        },
    )

    pd.testing.assert_frame_equal(symbols, loaded_symbols)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_symbols_data_to_dw(asset_category, drop_dw_tables):
    symbols = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.csv")
    )

    load_to_dw(symbols, "symbols", asset_category)

    loaded_data = pd.read_sql_table(f"symbols_{asset_category}", con=engine)

    pd.testing.assert_frame_equal(symbols, loaded_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_update_symbols_data_on_s3(asset_category, remove_s3_objects):
    # Load historical data
    symbols = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.csv")
    )
    load_to_s3(symbols, "symbols", asset_category)

    # Load update
    symbols_update = symbols.copy()
    if asset_category == "sp_stocks":
        symbols_update["date_stamp"] = "2026-01-02"
    load_to_s3(symbols_update, "symbols", asset_category)

    # Verify
    expected_data = (
        pd.concat([symbols, symbols_update]).sort_values(["symbol", "date_stamp"]).reset_index(drop=True)
        if asset_category == "sp_stocks"
        else symbols.sort_values("symbol").reset_index(drop=True)
    )

    loaded_symbols = pd.read_parquet(
        f"{DATA_PATH}/symbols/{asset_category}.parquet",
        storage_options={
            "key": AWS_ACCESS_KEY,
            "secret": AWS_SECRET_KEY,
            "endpoint_url": S3_ENDPOINT,
        },
    )

    pd.testing.assert_frame_equal(expected_data, loaded_symbols)

@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_update_symbols_data_on_dw(asset_category, drop_dw_tables):
    # load historical data
    symbols = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_symbols.csv")
    )
    load_to_dw(symbols, "symbols", asset_category)

    # Load updates
    symbols_update = symbols.copy()
    if asset_category == "sp_stocks":
        symbols_update["date_stamp"] = "2026-01-02"
    load_to_dw(symbols_update, "symbols", asset_category)

    # Verify
    expected_data = (
        pd.concat([symbols, symbols_update]).sort_values(["symbol", "date_stamp"]).reset_index(drop=True)
        if asset_category == "sp_stocks"
        else symbols.sort_values("symbol").reset_index(drop=True)
    )
    loaded_data = pd.read_sql_table(f"symbols_{asset_category}", con=engine)
    loaded_data = (
        loaded_data.sort_values(["symbol", "date_stamp"]).reset_index(drop=True)
        if asset_category == "sp_stocks"
        else loaded_data.sort_values("symbol").reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(expected_data, loaded_data)

@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_price_data_to_s3(asset_category, remove_s3_objects):
    price_df = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv"),
    )
    price_df["date"] = pd.to_datetime(price_df["date"]).astype("datetime64[us]")

    load_to_s3(price_df, "price_history", asset_category)

    loaded_price_df = pd.read_parquet(
        f"{DATA_PATH}/price_history/{asset_category}.parquet",
        storage_options={
            "key": AWS_ACCESS_KEY,
            "secret": AWS_SECRET_KEY,
            "endpoint_url": S3_ENDPOINT,
        },
    )

    pd.testing.assert_frame_equal(price_df, loaded_price_df)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_load_price_data_to_dw(asset_category, drop_dw_tables):
    price_df = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv")
    )

    load_to_dw(price_df, "price_history", asset_category)

    loaded_data = pd.read_sql_table(f"price_history_{asset_category}", con=engine)

    pd.testing.assert_frame_equal(price_df, loaded_data)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_update_price_on_s3(asset_category, remove_s3_objects):
    # Load historical price
    hist_price_df = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv")
    )
    load_to_s3(hist_price_df, "price_history", asset_category)

    # Load price update
    price_update = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"{asset_category}_price_data_update.csv")
    )
    load_to_s3(price_update, "price_history", asset_category)

    # Verify merged data
    expected_df = (
        pd.concat([hist_price_df, price_update], ignore_index=True)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )

    loaded_price_df = pd.read_parquet(
        f"{DATA_PATH}/price_history/{asset_category}.parquet",
        storage_options={
            "key": AWS_ACCESS_KEY,
            "secret": AWS_SECRET_KEY,
            "endpoint_url": S3_ENDPOINT,
        },
    )

    pd.testing.assert_frame_equal(loaded_price_df, expected_df)


@pytest.mark.parametrize("asset_category", ("fx", "sp_stocks"))
def test_update_price_on_dw(asset_category, drop_dw_tables):
    # Load historical price
    hist_price_df = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"processed_{asset_category}_prices.csv")
    )
    load_to_dw(hist_price_df, "price_history", asset_category)

    # Load price update with existing records
    price_update = pd.read_csv(
        TEST_DATA_DIR.joinpath(f"{asset_category}_price_data_update.csv")
    )
    price_update_with_existing = pd.concat(
        [hist_price_df, price_update], ignore_index=True
    )
    load_to_dw(price_update_with_existing, "price_history", asset_category)

    # Verify merged data
    expected_df = (
        pd.concat([hist_price_df, price_update], ignore_index=True)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )

    loaded_price_df = (
        pd.read_sql_table(f"price_history_{asset_category}", con=engine)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(loaded_price_df, expected_df)


if __name__ == "__main__":
    pytest.main([__file__])
