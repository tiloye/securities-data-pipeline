import datetime as dt
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from py_pipeline.extract import extract, log_failed_dowloads, YF_ERRORS
from py_pipeline.load import load
from py_pipeline.transform import transform


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


@task(log_prints=True)
def extract_task(dataset: str, asset_category: str, source: str, **kwargs):
    return extract(dataset, asset_category, source, **kwargs)


@task(log_prints=True)
def transform_task(df: pd.DataFrame, dataset: str, asset_category: str, **kwargs):
    return transform(df, dataset, asset_category, **kwargs)


@task(log_prints=True)
def load_task(df: pd.DataFrame, dataset: str, asset_category: str, destination: str):
    return load(df, dataset, asset_category, destination)


def etl_symbols_source_to_s3(asset_category: str, **t_kwargs):
    print(f"Running ETL for {asset_category} symbols from source")
    df = extract_task(dataset="symbols", asset_category=asset_category, source="source")
    df = transform_task(
        df=df, dataset="symbols", asset_category=asset_category, **t_kwargs
    )
    load_task(df=df, dataset="symbols", asset_category=asset_category, destination="s3")


def etl_price_history_source_to_s3(
    asset_category: str,
    symbols: list[str],
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    chunk_size: int = 500,
):
    def _etl_price_history_source_to_s3(
        asset_category: str,
        symbols: list[str],
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
    ):
        df = extract_task(
            dataset="price_history",
            asset_category=asset_category,
            source="source",
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
        )
        log_failed_dowloads(asset_category)
        df = transform_task(
            df=df, dataset="price_history", asset_category=asset_category
        )
        if df.empty:  # Source system returns empty datafram if that is unavailable
            return
        load_task(
            df=df,
            dataset="price_history",
            asset_category=asset_category,
            destination="s3",
        )

    if len(symbols) > chunk_size:
        print(
            f"Running ETL for {asset_category} price history from source in chunks of {chunk_size}"
        )
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i : i + chunk_size]
            _etl_price_history_source_to_s3(
                asset_category=asset_category,
                symbols=chunk,
                start_date=start_date,
                end_date=end_date,
            )
    else:
        _etl_price_history_source_to_s3(
            asset_category=asset_category,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
        )

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


def el_symbols_s3_to_dw(
    asset_category: str,
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
):
    print(f"Running EL for {asset_category} symbols to DW")
    df = extract_task(
        dataset="symbols",
        asset_category=asset_category,
        source="s3",
        symbols_only=False,
        start_date=start_date,
        end_date=end_date,
    )
    load_task(df=df, dataset="symbols", asset_category=asset_category, destination="dw")


def el_price_history_s3_to_dw(
    asset_category: str, start_date: dt.date | None, end_date: dt.date | None
):
    print(f"Running EL for {asset_category} price history to DW")
    df = extract_task(
        dataset="price_history",
        asset_category=asset_category,
        source="s3",
        start_date=start_date,
        end_date=end_date,
    )
    load_task(
        df=df, dataset="price_history", asset_category=asset_category, destination="dw"
    )


@flow(log_prints=True)
def etl_flow(
    asset_category: str,
    symbols: list[str] | None = None,
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    chunk_size: int = 500,
):

    start_date, end_date = get_start_end_dates(start_date, end_date)

    if symbols is None:
        # The source is assumed to provide the current, up-to-date list of symbols.
        # We stamp this data to align with the price history being extracted.
        # Note: During a historical backfill, this will result in today's
        # symbols being stamped with an older date.
        date_stamp = end_date - dt.timedelta(days=1)
        etl_symbols_source_to_s3(asset_category, date_stamp=date_stamp)

    # S3 Price History ETL
    symbols = (
        extract_task(
            dataset="symbols",
            asset_category=asset_category,
            source="s3",
            symbols_only=True,
        )
        if symbols is None
        else symbols
    )

    try:
        etl_price_history_source_to_s3(
            asset_category=asset_category,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            chunk_size=chunk_size,
        )
    except RuntimeError as e:
        if len(YF_ERRORS[asset_category]) < len(symbols):
            el_symbols_s3_to_dw(
                asset_category=asset_category, start_date=start_date, end_date=end_date
            )
            el_price_history_s3_to_dw(
                asset_category=asset_category, start_date=start_date, end_date=end_date
            )
        raise e
    else:
        el_symbols_s3_to_dw(
            asset_category=asset_category, start_date=start_date, end_date=end_date
        )
        el_price_history_s3_to_dw(
            asset_category=asset_category, start_date=start_date, end_date=end_date
        )


@task(log_prints=True)
def create_dbt_profiles(project_dir: Path) -> None:
    from py_pipeline.config import (
        DB_HOST,
        DB_PORT,
        DB_USER,
        DB_PASSWORD,
        DB_NAME,
        DB_TYPE,
    )

    print(f"Creating profiles.yml for {DB_TYPE}")

    if DB_TYPE == "postgres":
        profiles_content = f"""sec_dw_transformer:
    outputs:
        dev:
            type: {DB_TYPE}
            dbname: {DB_NAME}
            user: {DB_USER}
            pass: {DB_PASSWORD}
            host: {DB_HOST}
            port: {DB_PORT}
            schema: public
            threads: 1
    target: dev"""
    elif DB_TYPE == "snowflake":
        profiles_content = f"""sec_dw_transformer:
    outputs:
        dev:
            type: {DB_TYPE}
            account: {DB_HOST}
            user: {DB_USER}
            password: {DB_PASSWORD}
            database: {DB_NAME}
            schema: public
            warehouse: "COMPUTE_WH"
            threads: 1
    target: dev"""
    else:
        raise ValueError(f"Unknown database type: {DB_TYPE}")

    profiles_path = project_dir / "profiles.yml"
    with open(profiles_path, "w") as f:
        f.write(profiles_content)

    print(f"Created/updated profiles.yml at {profiles_path}")


@flow(log_prints=True)
def dbt_runner() -> None:
    print("Running dbt")

    dbt_project_path = Path(__file__).parent.parent / "dw_transformer"
    create_dbt_profiles(dbt_project_path)
    settings = PrefectDbtSettings(
        project_dir=dbt_project_path, profiles_dir=dbt_project_path
    )

    runner = PrefectDbtRunner(settings=settings)
    runner.invoke(["deps"])
    runner.invoke(["run"])
    runner.invoke(["test"])


if __name__ == "__main__":
    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=30)

    etl_flow(asset_category="fx", start_date=start_date, end_date=end_date)
    etl_flow(asset_category="sp_stocks", start_date=start_date, end_date=end_date)
    dbt_runner()
