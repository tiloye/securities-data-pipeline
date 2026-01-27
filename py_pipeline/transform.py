import pandas as pd
from prefect import task

######## Symbol data transformers ########


@task
def transform_stocks_symbol_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)
    df.columns = df.columns.str.lower()
    df.rename(
        columns={
            "security": "name",
            "gics sector": "sector",
            "gics sub-industry": "industry",
        },
        inplace=True,
    )
    df["symbol"] = df["symbol"].str.replace(".", "-")
    df["sector"].fillna("Missing", inplace=True)
    df["industry"].fillna("Missing", inplace=True)
    return df[["symbol", "name", "sector", "industry"]]


def transform_fx_symbol_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.lower()
    return df


######### Price data transformers #########


@task
def transform_price_df(df: pd.DataFrame, asset_category: str) -> pd.DataFrame:
    if df.empty:
        return df

    cols_without_data = df.columns[df.isna().sum() == df.shape[0]]

    df = df.drop(cols_without_data, axis=1)
    df = df.stack("Ticker", future_stack=True).reset_index()
    df.columns = df.columns.str.lower().rename(None)
    df["date"] = df["date"].dt.tz_localize(None).astype("datetime64[us]")
    df.rename(columns={"ticker": "symbol"}, inplace=True)
    if asset_category == "fx":
        df["symbol"] = (
            df["symbol"]
            .str.replace("=X", "")
            .replace({"CHF": "USDCHF", "CAD": "USDCAD", "JPY": "USDJPY"})
        )
    return df
