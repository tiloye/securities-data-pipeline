import datetime as dt

import pandas as pd
import yfinance as yf

from .config import AWS_ACCESS_KEY, AWS_SECRET_KEY, DATA_PATH, S3_ENDPOINT

######### Symbols data extractors #########


def get_sp_stock_symbols_from_source() -> pd.DataFrame:
    cols = ["Symbol", "Security", "GICS Sector", "GICS Sub-Industry"]
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_{}_companies"
    storage_options = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
    }
    sp_stocks = pd.concat(
        [pd.read_html(url.format(index), storage_options=storage_options)[0] for index in [400, 500, 600]]
    )

    return sp_stocks[cols]


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


def get_symbols_from_s3(
    asset_category: str, symbols_only: bool = True
) -> list[str] | pd.DataFrame:
    """Extract symbols data from the object store."""

    s3_storage_options = {
        "key": AWS_ACCESS_KEY,
        "secret": AWS_SECRET_KEY,
        "endpoint_url": S3_ENDPOINT,
    }

    path = f"{DATA_PATH}/symbols/{asset_category}"
    symbols = pd.read_csv(f"{path}.csv", storage_options=s3_storage_options)

    if symbols_only:
        symbols = symbols["symbol"].to_list()
        return symbols
    return symbols


######### Price data extractors #########

YF_ERRORS = {"fx": [], "sp_stocks": []}


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


def get_prices_from_s3(
    asset_category: str,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
) -> pd.DataFrame | None:
    """Extract historical price data from the object store."""

    s3_storage_options = {
        "key": AWS_ACCESS_KEY,
        "secret": AWS_SECRET_KEY,
        "endpoint_url": S3_ENDPOINT,
    }

    path = f"{DATA_PATH}/price_history/{asset_category}.parquet"

    if start and end:
        price_data = pd.read_parquet(
            path,
            storage_options=s3_storage_options,
            filters=[
                ("date", ">=", start),
                ("date", "<=", end),
            ],
        ).query(f"date >= '{start}' and date <= '{end}'")
    else:
        price_data = pd.read_parquet(path, storage_options=s3_storage_options)
    return price_data
