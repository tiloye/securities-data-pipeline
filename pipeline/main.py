import datetime as dt

import duckdb
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact

import price_history as price_history
import utils.s3_el as s3_el


@task
def get_symbols(asset_category: str) -> list[str]:
    try:
        symbols = s3_el.extract("symbols", asset_category)
        return symbols
    except duckdb.IOException:
        import symbols

        symbols.etl()
        return s3_el.extract("symbols", asset_category)


@task
def get_hist_start_end_dates(asset_category: str) -> str | dt.date:
    try:
        price_history = s3_el.extract("price_history", asset_category)
        dates = (
            duckdb.sql("SELECT date FROM price_history ORDER BY date").df().squeeze()
        )
        start_date = dates.iloc[-1] + dt.timedelta(days=1)
        end_date = dt.datetime.now().date()
        return start_date, end_date
    except duckdb.IOException:
        start_date = dt.date(2000, 1, 1)
        end_date = dt.datetime.now().date()
        return start_date, end_date


@flow
def etl(
    asset_category: str | list[str] = ["fx", "sp_stocks"],
    symbols: list[str] | None = None,
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    chunk_size: int = 500,
):
    if isinstance(asset_category, str) and asset_category not in ["fx", "sp_stocks"]:
        raise ValueError(
            f"'asset_category should be one of 'fx' or 'sp_stocks', got {asset_category}"
        )

    if isinstance(asset_category, list):
        for category in asset_category:
            symbols = get_symbols(category) if not symbols else symbols
            start_date, end_date = (
                get_hist_start_end_dates(category)
                if not (start_date and end_date)
                else (start_date, end_date)
            )
            price_history.etl_bars(
                category, symbols, start_date, end_date, chunk_size
            )
            symbols = None
    else:
        symbols = get_symbols(asset_category) if not symbols else symbols
        start_date, end_date = (
            get_hist_start_end_dates(asset_category)
            if not (start_date and end_date)
            else (start_date, end_date)
        )
        price_history.etl_bars(
            asset_category, symbols, start_date, end_date, chunk_size
        )

    create_markdown_artifact(
        f"""
        Downloaded {asset_category if isinstance(asset_category, str) else ", ".join(asset_category)} price history from {start_date} to {end_date}.
        Failed downloads: {price_history.YF_ERRORS}
        """,
        description="YF Result",
    )


if __name__ == "__main__":
    etl.serve(name="securities-data-pipeline", cron="0 0 * * 1-5")
