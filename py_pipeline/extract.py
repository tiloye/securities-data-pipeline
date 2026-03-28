import datetime as dt

import pandas as pd
import yfinance as yf
from deltalake import DeltaTable

from py_pipeline.config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    DATA_PATH,
    S3_ENDPOINT,
    ENV_NAME,
)


def extract(
    dataset: str, asset_category: str, source: str = "source", **kwargs
) -> pd.DataFrame | list[str]:
    if source == "source":
        if dataset == "symbols":
            if asset_category == "sp_stocks":
                return get_sp_stock_symbols_from_source()
            elif asset_category == "fx":
                return get_fx_symbols_from_source()
            else:
                raise ValueError(f"Unknown asset category: {asset_category}")
        elif dataset == "price_history":
            return get_prices_from_source(**kwargs)
        else:
            raise ValueError(f"Unknown dataset: {dataset}")
    elif source == "s3":
        if dataset == "symbols":
            return get_symbols_from_s3(asset_category=asset_category, **kwargs)
        elif dataset == "price_history":
            return get_prices_from_s3(asset_category=asset_category, **kwargs)
        else:
            raise ValueError(f"Unknown dataset: {dataset}")
    else:
        raise ValueError(f"Unknown source: {source}")


######### Symbols data extractors #########


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
    asset_category: str,
    symbols_only: bool = True,
    start_date: dt.date | str | None = None,
    end_date: dt.date | str | None = None,
) -> list[str] | pd.DataFrame:
    """Extract symbols data from the object store."""

    columns = ["symbol"] if symbols_only else None
    filters = None

    if asset_category == "sp_stocks" and (start_date and end_date):
        filters = [
            ("date_stamp", ">=", pd.Timestamp(start_date).date()),
            ("date_stamp", "<=", pd.Timestamp(end_date).date()),
        ]

    df = _get_data_from_s3(asset_category, "symbols", filters=filters, columns=columns)

    return df["symbol"].unique().tolist() if symbols_only else df


def _get_data_from_s3(
    asset_category: str,
    data_set: str,
    columns: list[str] | None = None,
    filters: list[str, str] | None = None,
) -> pd.DataFrame:
    """Helper to centralize S3 storage options and parquet reading."""
    s3_storage_options = {
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
        "AWS_ENDPOINT_URL": S3_ENDPOINT,
        "AWS_ALLOW_HTTP": "true",
    }
    path = f"{DATA_PATH}/{data_set}/{asset_category}"

    return DeltaTable(path, storage_options=s3_storage_options).to_pandas(
        columns=columns, filters=filters
    )


YF_ERRORS = {"fx": [], "sp_stocks": []}


def get_prices_from_source(
    symbols: list[str],
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
) -> pd.DataFrame:
    bars = yf.download(symbols, start=start_date, end=end_date, auto_adjust=True)
    return bars


def log_failed_dowloads(asset_category: str) -> None:
    symbols_with_errors = yf.shared._ERRORS
    if symbols_with_errors:
        YF_ERRORS[asset_category].extend(list(symbols_with_errors.keys()))


def get_prices_from_s3(
    asset_category: str,
    start_date: dt.date | str | None = None,
    end_date: dt.date | str | None = None,
) -> pd.DataFrame:
    """Extract historical price data from the object store."""

    filters = None
    if start_date and end_date:
        filters = [
            ("date", ">=", pd.Timestamp(start_date).date()),
            ("date", "<=", pd.Timestamp(end_date).date()),
        ]

    return _get_data_from_s3(asset_category, "price_history", filters=filters)
