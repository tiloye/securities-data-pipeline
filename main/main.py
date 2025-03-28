import datetime as dt

import duckdb
from prefect import flow, task

from pipeline import price_history as ph, s3_el


@task
def get_symbols(asset_category: str) -> list[str]:
    try:
        symbols = s3_el.extract("symbols", asset_category)
        return symbols
    except duckdb.IOException:
        from pipeline import symbols

        symbols.etl(asset_category)
        return s3_el.extract("symbols", asset_category)


@task
def get_hist_start_end_dates(asset_category: str) -> dt.date:
    try:
        prices = s3_el.extract("price_history", asset_category)
        dates = duckdb.sql("SELECT date FROM prices ORDER BY date").df().squeeze()
        start_date = dates.iloc[-1] + dt.timedelta(days=1)
        end_date = dt.datetime.now().date()
        return start_date, end_date
    except duckdb.IOException:
        start_date = dt.date(2000, 1, 1)
        end_date = dt.datetime.now().date()
        return start_date, end_date


@flow(name="Securities Data ETL", log_prints=True)
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

    symbols = get_symbols(asset_category) if not symbols else symbols
    start_date, end_date = (
        get_hist_start_end_dates(asset_category)
        if not (start_date and end_date)
        else (start_date, end_date)
    )
    ph.etl_bars(asset_category, symbols, start_date, end_date, chunk_size)

    if ph.YF_ERRORS[asset_category]:
        raise RuntimeError(
            f"""
            Failed to get data for some symbols
            Asset category: {asset_category}
            Symbols: {str(ph.YF_ERRORS[asset_category]).replace("'", "\"")}
            Start date: {start_date}
            End date: {end_date}
            """
        )


if __name__ == "__main__":
    etl("fx")
    etl("sp_stocks")
