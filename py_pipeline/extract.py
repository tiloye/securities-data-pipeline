import datetime as dt

import pandas as pd
import yfinance as yf
from prefect import task

from py_pipeline.config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    DATA_PATH,
    S3_ENDPOINT,
    ENV_NAME,
)


######### Helper functions #########


def _get_data_from_s3(
    asset_category: str,
    data_set: str,
    columns: list[str] | None = None,
    filters: list[str, str] | None = None,
) -> pd.DataFrame:
    """Helper to centralize S3 storage options and parquet reading."""
    s3_storage_options = {
        "key": AWS_ACCESS_KEY,
        "secret": AWS_SECRET_KEY,
        "endpoint_url": S3_ENDPOINT,
    }
    path = f"{DATA_PATH}/{data_set}/{asset_category}.parquet"

    return pd.read_parquet(
        path, storage_options=s3_storage_options, filters=filters, columns=columns
    )


######### Symbols data extractors #########


@task
def get_sp_stock_symbols_from_source() -> pd.DataFrame:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_{}_companies"
    storage_options = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
    }

    sp_400 = pd.read_html(url.format(400), storage_options=storage_options)[0].assign(
        in_sp400=True
    )
    sp_500 = pd.read_html(url.format(500), storage_options=storage_options)[0].assign(
        in_sp500=True
    )
    sp_600 = pd.read_html(url.format(600), storage_options=storage_options)[0].assign(
        in_sp600=True
    )
    sp_stocks = pd.concat([sp_400, sp_500, sp_600])

    if ENV_NAME == "dev":
        sp_stocks = sp_stocks.sample(5)
    return sp_stocks


@task
def get_fx_symbols_from_source() -> pd.DataFrame:
    fx_symbols = [
        "EURUSD=X",
        "GBPUSD=X",
        "AUDUSD=X",
        "NZDUSD=X",
        "JPY=X",
        "CHF=X",
        "CAD=X",
    ]
    return pd.Series(fx_symbols, name="Symbol").to_frame()


@task
def get_symbols_from_s3(
    asset_category: str,
    symbols_only: bool = True,
    start: dt.date | str | None = None,
    end: dt.date | str | None = None,
) -> list[str] | pd.DataFrame:
    """Extract symbols data from the object store."""

    columns = ["symbol"] if symbols_only else None
    filters = None

    if asset_category != "fx" and start and end:
        filters = [
            ("date_stamp", ">=", pd.Timestamp(start).date()),
            ("date_stamp", "<=", pd.Timestamp(end).date()),
        ]

    df = _get_data_from_s3(
        asset_category, "symbols", filters=filters, columns=columns
    )

    return df["symbol"].unique().tolist() if symbols_only else df


######### Price data extractors #########

YF_ERRORS = {"fx": [], "sp_stocks": []}


@task
def get_prices_from_source(
    symbols: list[str],
    start: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
) -> pd.DataFrame:
    bars = yf.download(
        symbols, start=start, end=end_date, group_by="ticker", auto_adjust=True
    )
    return bars


def log_failed_dowloads(asset_category: str) -> None:
    symbols_with_errors = yf.shared._ERRORS
    if symbols_with_errors:
        YF_ERRORS[asset_category].extend(list(symbols_with_errors.keys()))


@task
def get_prices_from_s3(
    asset_category: str,
    start: dt.date | str | None = None,
    end: dt.date | str | None = None,
) -> pd.DataFrame:
    """Extract historical price data from the object store."""
    
    filters = None
    if start and end:
        filters = [
            ("date", ">=", pd.Timestamp(start).date()),
            ("date", "<=", pd.Timestamp(end).date()),
        ]

    return _get_data_from_s3(asset_category, "price_history", filters=filters)
