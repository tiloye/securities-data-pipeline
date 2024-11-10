import pandas as pd
import s3_el
from prefect import task

FX_SYMBOLS = ["EURUSD=X", "GBPUSD=X", "AUDUSD=X", "NZDUSD=X", "JPY=X", "CHF=X", "CAD=X"]


@task
def get_sp_stock_symbols() -> pd.DataFrame:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_{}_companies"
    sp_stocks = pd.concat(
        [pd.read_html(url.format(index))[0] for index in [400, 500, 600]]
    )
    return sp_stocks


@task
def transform_stocks_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.lower()
    df["name"] = df["security"].combine_first(df["company"])
    df.rename(
        columns={"gics sector": "sector", "gics sub-industry": "industry"}, inplace=True
    )
    df["symbol"] = df["symbol"].str.replace(".", "-")
    return df[["symbol", "name", "sector", "industry"]]


@task
def load_sp_stock_symbols() -> None:
    stock_symbols_df = get_sp_stock_symbols()
    stock_symbols_df = transform_stocks_df(stock_symbols_df)
    s3_el.load(stock_symbols_df, "symbols", "sp_stocks")
    print(f"Successfully loaded symbols data for {len(stock_symbols_df)} stocks.")


@task(log_prints=True)
def load_fx_symbols() -> None:
    fx_symbols_df = pd.DataFrame(FX_SYMBOLS, columns=["symbol"])
    s3_el.load(fx_symbols_df, "symbols", "fx")
    print(f"Successfully loaded symbols data for {len(fx_symbols_df)} forex pairs.")

@task(log_prints=True)
def load_symbols(asset_category: str) -> None:
    if asset_category == "fx":
        load_fx_symbols()
    elif asset_category == "sp_stocks":
        load_sp_stock_symbols()
    else:
        raise ValueError(f"Unknown asset category, {asset_category}")

if __name__ == "__main__":
    for asset_category in ["fx", "sp_stocks"]:
        load_symbols(asset_category)
