from __future__ import annotations

import argparse
import ast
import datetime as dt

import pandas as pd
from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from py_pipeline.extract import (
    YF_ERRORS,
    get_fx_symbols_from_source,
    get_prices_from_source,
    get_sp_stock_symbols_from_source,
    get_symbols_from_s3,
    get_prices_from_s3,
    log_failed_dowloads,
)
from py_pipeline.load import load_to_s3, load_to_dw
from py_pipeline.transform import (
    transform_fx_symbol_df,
    transform_price_df,
    transform_stocks_symbol_df,
)


############## Symbols ETL ##############


def etl_fx_symbols_to_s3() -> None:
    fx_symbols_df = get_fx_symbols_from_source()
    fx_symbols_df = transform_fx_symbol_df(fx_symbols_df)
    load_to_s3(fx_symbols_df, "symbols", "fx")
    print(f"Successfully loaded symbols data for {len(fx_symbols_df)} forex pairs.")


def etl_sp_stocks_symbols_to_s3() -> None:
    stock_symbols_df = get_sp_stock_symbols_from_source()
    stock_symbols_df = transform_stocks_symbol_df(stock_symbols_df)
    load_to_s3(stock_symbols_df, "symbols", "sp_stocks")
    print(f"Successfully loaded symbols data for {len(stock_symbols_df)} stocks.")


@flow
def etl_symbols_to_s3(asset_category: str) -> None:
    if asset_category == "fx":
        etl_fx_symbols_to_s3()
    else:
        etl_sp_stocks_symbols_to_s3()


@flow
def el_symbols_to_dw(asset_category: str) -> None:
    symbols_df = get_symbols_from_s3(asset_category, symbols_only=False)
    load_to_dw(symbols_df, "symbols", asset_category)

    print(f"Successfully loaded {asset_category} symbols data to the database.")


############# Price History ETL #############


def et_price_history_from_source(
    asset_category: str,
    symbols: list[str],
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
) -> pd.DataFrame:
    price_history = get_prices_from_source(symbols, start_date, end_date)
    log_failed_dowloads(asset_category)
    price_history = transform_price_df(price_history, asset_category)
    return price_history


def etl_bars_in_chunk(
    asset_category: str,
    symbols: list[str],
    start_date: str | dt.date | None,
    end_date: str | dt.date | None,
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


@flow
def etl_bars_to_s3(
    asset_category: str,
    symbols: list[str],
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
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


@flow
def el_bars_to_dw(
    asset_category: str,
    start: str | dt.date | None = None,
    end: str | dt.date | None = None,
) -> None:
    if start and end:
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)
    prices_df = get_prices_from_s3(asset_category, start, end)
    load_to_dw(prices_df, "price_history", asset_category)

    print(f"Successfully loaded {asset_category} price history data to the database.")


############### Combined ETL ##############


def get_start_end_dates(
    start_date: str | dt.date | None = None, end_date: str | dt.date | None = None
) -> tuple[dt.date | None, dt.date | None]:
    if start_date or end_date:
        if isinstance(start_date, str):
            start_date = dt.date.fromisoformat(start_date)

        if isinstance(end_date, str):
            end_date = dt.date.fromisoformat(end_date)

        if start_date is None and end_date is not None:
            start_date = dt.date(2000, 1, 1)
        elif end_date is None and start_date is not None:
            end_date = dt.date.today()
    else:
        end_date = dt.date.today()
        start_date = end_date - dt.timedelta(days=1)

    return start_date, end_date


@flow
def etl_s3(
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
        etl_symbols_to_s3(asset_category)
        symbols = get_symbols_from_s3(asset_category)

    start_date, end_date = get_start_end_dates(start_date, end_date)

    etl_bars_to_s3(asset_category, symbols, start_date, end_date, chunk_size)

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


@flow
def el_dw(asset_category: str):
    el_symbols_to_dw(asset_category)
    el_bars_to_dw(asset_category)


@flow(log_prints=True)
def main_fx(
    symbols: list[str] | None = None,
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    chunk_size: int = 500,
) -> None:
    try:
        etl_s3(
            "fx",
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            chunk_size=chunk_size,
        )
    except RuntimeError as e:
        print(f"ETL Warning: {e}")

    el_dw("fx")


@flow(log_prints=True)
def main_sp_stocks(
    symbols: list[str] | None = None,
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    chunk_size: int = 500,
) -> None:
    try:
        etl_s3(
            "sp_stocks",
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            chunk_size=chunk_size,
        )
    except RuntimeError as e:
        print(f"ETL Warning: {e}")
    el_dw("sp_stocks")


@flow
def dbt_runner() -> None:
    from pathlib import Path

    dbt_project_path = Path(__file__).parent.parent / "dw_transformer"
    settings = PrefectDbtSettings(project_dir=dbt_project_path)

    runner = PrefectDbtRunner(settings=settings)
    runner.invoke(["deps"])
    runner.invoke(["run"])
    runner.invoke(["test"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Securities Data Pipeline ETL")
    parser.add_argument(
        "--asset-category",
        type=str,
        choices=["fx", "sp_stocks"],
        required=False,
        help="Asset category to process (fx or sp_stocks). If not provided, both will be processed.",
    )
    parser.add_argument(
        "--symbols",
        type=ast.literal_eval,
        help="List of symbols to process.",
        default=None,
    )
    parser.add_argument(
        "--start-date",
        type=dt.date.fromisoformat,
        required=False,
        help="Start date for price data in YYYY-MM-DD format. Defaults to yesterday if not provided.",
    )
    parser.add_argument(
        "--end-date",
        type=dt.date.fromisoformat,
        required=False,
        help="End date for price data in YYYY-MM-DD format. Defaults to today if not provided.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Number of symbols to process in each chunk when fetching price data.",
    )

    args = parser.parse_args()
    symbols = args.symbols
    start_date = args.start_date
    end_date = args.end_date
    chunk_size = args.chunk_size

    if args.asset_category == "fx":
        main_fx(symbols, start_date, end_date, chunk_size)
    elif args.asset_category == "sp_stocks":
        main_sp_stocks(symbols, start_date, end_date, chunk_size)
    else:
        main_fx(symbols, start_date, end_date)
        main_sp_stocks(symbols, start_date, end_date, chunk_size)


if __name__ == "__main__":
    start_date = "2025-01-01"
    end_date = "2025-01-31"

    main_fx(start_date=start_date, end_date=end_date)
    main_sp_stocks(start_date=start_date, end_date=end_date)
    dbt_runner()
