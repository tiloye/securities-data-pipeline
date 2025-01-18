import datetime as dt
import time

import pandas as pd
import yfinance as yf
from prefect import flow, task

import utils.s3_el as s3_el

YF_ERRORS = {"fx": [], "sp_stocks": []}


@task
def get_daily_prices(
    symbols: list[str],
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
) -> pd.DataFrame:
    bars = yf.download(
        symbols, start=start_date, end=end_date, group_by="ticker", auto_adjust=True
    )
    return bars


@task(log_prints=True)
def check_errors(asset_category: str) -> None:
    symbols_with_errors = yf.shared._ERRORS
    if symbols_with_errors:
        YF_ERRORS[asset_category].extend(list(symbols_with_errors.keys()))


@task
def transform(df: pd.DataFrame, asset_category: str) -> pd.DataFrame | None:
    if df.empty:
        return
    
    failed_symbols = df.columns.get_level_values(0)[
        df.columns.get_level_values(0).isin(YF_ERRORS[asset_category])
    ]
    df = df.drop(failed_symbols, axis=1, level=0)
    df = df.stack("Ticker", future_stack=True).reset_index()
    df.columns = df.columns.str.lower().rename(None)
    df["date"] = df["date"].dt.date
    df.rename(columns={"ticker": "symbol"}, inplace=True)
    if asset_category == "fx":
        df["symbol"] = (
            df["symbol"]
            .str.replace("=X", "")
            .replace({"CHF": "USDCHF", "CAD": "USDCAD", "JPY": "USDJPY"})
        )
    return df


@flow
def et_price_history_from_source(
    asset_category: str,
    symbols: list[str],
    start_date: str | dt.datetime,
    end_date: str | dt.datetime,
) -> pd.DataFrame:
    price_history = get_daily_prices(symbols, start_date, end_date)
    check_errors(asset_category)
    price_history = transform(price_history, asset_category)
    return price_history


@flow
def etl_bars_in_chunk(
    asset_category: str,
    symbols: list[str],
    start_date: dt.date,
    end_date: dt.date,
    chunk_size: int,
) -> None:
    for idx in range(0, len(symbols), chunk_size):
        symbols_subset = symbols[idx : idx + chunk_size]
        price_history = et_price_history_from_source(
            asset_category, symbols_subset, start_date, end_date
        )
        s3_el.load(price_history, "price_history", asset_category)
        time.sleep(5)


@flow(log_prints=True)
def etl_bars(
    asset_category: str,
    symbols: list[str],
    start_date: str | dt.date,
    end_date: str | dt.date,
    chunk_size: int = 500,
) -> None:
    print(f"Requesting {asset_category} price from {start_date} to {end_date}")

    if len(symbols) > chunk_size:
        etl_bars_in_chunk(
            asset_category, symbols, start_date, end_date, chunk_size
        )
    else:
        price_history = et_price_history_from_source(
            asset_category, symbols, start_date, end_date
        )
        s3_el.load(price_history, "price_history", asset_category)

    print(
        f"Successfully loaded {asset_category} price history from {start_date} to {end_date}."
    )
