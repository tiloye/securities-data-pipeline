from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

from pipeline.extract import (
    YF_ERRORS,
    get_fx_symbols_from_source,
    get_prices_from_source,
    get_sp_stock_symbols_from_source,
    get_symbols_from_s3,
    log_failed_dowloads,
)
from pipeline.load import load_to_s3
from pipeline.transform import (
    transform_fx_symbol_df,
    transform_price_df,
    transform_stocks_symbol_df,
)

if TYPE_CHECKING:
    import pandas as pd


############## Symbols ETL ##############


def etl_fx_symbols() -> None:
    fx_symbols_df = get_fx_symbols_from_source()
    fx_symbols_df = transform_fx_symbol_df(fx_symbols_df)
    load_to_s3(fx_symbols_df, "symbols", "fx")
    print(f"Successfully loaded symbols data for {len(fx_symbols_df)} forex pairs.")


def etl_sp_stocks_symbols() -> None:
    stock_symbols_df = get_sp_stock_symbols_from_source()
    stock_symbols_df = transform_stocks_symbol_df(stock_symbols_df)
    load_to_s3(stock_symbols_df, "symbols", "sp_stocks")
    print(f"Successfully loaded symbols data for {len(stock_symbols_df)} stocks.")


def etl_symbols(asset_category: str) -> None:
    if asset_category == "fx":
        etl_fx_symbols()
    else:
        etl_sp_stocks_symbols()


############# Price History ETL #############


def et_price_history_from_source(
    asset_category: str,
    symbols: list[str],
    start_date: str | dt.date,
    end_date: str | dt.date,
) -> pd.DataFrame:
    price_history = get_prices_from_source(symbols, start_date, end_date)
    log_failed_dowloads(asset_category)
    price_history = transform_price_df(price_history, asset_category)
    return price_history


def etl_bars_in_chunk(
    asset_category: str,
    symbols: list[str],
    start_date: str | dt.date,
    end_date: str | dt.date,
    chunk_size: int,
) -> None:
    for idx in range(0, len(symbols), chunk_size):
        symbols_subset = symbols[idx : idx + chunk_size]
        price_history = et_price_history_from_source(
            asset_category, symbols_subset, start_date, end_date
        )
        if price_history.empty:
            continue
        load_to_s3(price_history, "price_history", asset_category)
    if len(symbols) == len(YF_ERRORS[asset_category]):
        raise ValueError("Could not transform empty dataframes")


def etl_bars(
    asset_category: str,
    symbols: list[str],
    start_date: str | dt.date,
    end_date: str | dt.date,
    chunk_size: int = 500,
) -> None:
    print(f"Requesting {asset_category} price from {start_date} to {end_date}")

    if len(symbols) > chunk_size:
        etl_bars_in_chunk(asset_category, symbols, start_date, end_date, chunk_size)
    else:
        price_history = et_price_history_from_source(
            asset_category, symbols, start_date, end_date
        )
        if price_history.empty:
            raise ValueError("Tranformed dataframe is empty")
        load_to_s3(price_history, "price_history", asset_category)

    print(
        f"Successfully loaded {asset_category} price history from {start_date} to {end_date}."
    )


############### Combined ETL ##############


def get_start_end_dates() -> tuple[dt.date, dt.date]:
    end_date = dt.datetime.now(dt.timezone.utc).date()
    start_date = end_date - dt.timedelta(days=1)
    return start_date, end_date


def etl(
    asset_category: str,
    symbols: list[str] | None = None,
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    chunk_size: int = 500,
):
    if asset_category not in ["fx", "sp_stocks"]:
        raise ValueError(
            f"'asset_category should be one of 'fx' or 'sp_stocks', got {asset_category}"
        )

    try:
        symbols = symbols if symbols else get_symbols_from_s3(asset_category)
    except Exception:
        etl_symbols(asset_category)
        symbols = get_symbols_from_s3(asset_category)

    if not (start_date and end_date):
        start_date, end_date = get_start_end_dates()

    etl_bars(asset_category, symbols, start_date, end_date, chunk_size)

    if YF_ERRORS[asset_category]:
        raise RuntimeError(
            f"""
            Failed to get data for some symbols
            Asset category: {asset_category}
            Symbols: {str(YF_ERRORS[asset_category]).replace("'", '"')}
            Start date: {start_date}
            End date: {end_date}
            """
        )

if __name__ == "__main__":
    etl("fx")
    # etl("sp_stocks")
