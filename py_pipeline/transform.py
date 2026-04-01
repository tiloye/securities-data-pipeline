import datetime as dt
import pandas as pd
from py_pipeline.validate import (
    raw_stock_symbols_schema,
    raw_fx_symbols_schema,
    raw_price_schema,
)


def transform(
    df: pd.DataFrame,
    dataset: str,
    asset_category: str,
    **kwargs,
) -> pd.DataFrame:
    if dataset == "symbols":
        if asset_category == "sp_stocks":
            return transform_stocks_symbol_df(df, **kwargs)
        elif asset_category == "fx":
            return transform_fx_symbol_df(df)
        else:
            raise ValueError(f"Unknown asset category: {asset_category}")
    elif dataset == "price_history":
        return transform_price_df(df, asset_category)
    else:
        raise ValueError(f"Unknown dataset: {dataset}")


def transform_stocks_symbol_df(
    df: pd.DataFrame, date_stamp: str | dt.date
) -> pd.DataFrame:
    df = raw_stock_symbols_schema.validate(df, lazy=True).reset_index(drop=True)
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
    df.loc[df["sector"].isna(), "sector"] = "Missing"
    df.loc[df["industry"].isna(), "industry"] = "Missing"
    df.loc[df["in_sp400"].isna(), "in_sp400"] = False
    df.loc[df["in_sp500"].isna(), "in_sp500"] = False
    df.loc[df["in_sp600"].isna(), "in_sp600"] = False
    df[["in_sp400", "in_sp500", "in_sp600"]] = df[
        ["in_sp400", "in_sp500", "in_sp600"]
    ].astype(bool)
    df["date_stamp"] = pd.Timestamp(date_stamp).date()

    cols = [
        "symbol",
        "name",
        "sector",
        "industry",
        "in_sp400",
        "in_sp500",
        "in_sp600",
        "date_stamp",
    ]
    return df[cols]


def transform_fx_symbol_df(df: pd.DataFrame) -> pd.DataFrame:
    df = raw_fx_symbols_schema.validate(df, lazy=True)
    df.columns = df.columns.str.lower()
    return df


def transform_price_df(df: pd.DataFrame, asset_category: str) -> pd.DataFrame:
    if df.empty:
        return df

    df = raw_price_schema.validate(df, lazy=True)
    cols_without_data = df.columns[df.isna().sum() == df.shape[0]]

    df = df.drop(cols_without_data, axis=1)
    df = df.stack("Ticker", future_stack=True).reset_index()
    df.columns = df.columns.str.lower().rename(None)
    df["date"] = df["date"].dt.date
    df.rename(columns={"ticker": "symbol", "date": "date_stamp"}, inplace=True)
    if asset_category == "fx":
        df["symbol"] = (
            df["symbol"]
            .str.replace("=X", "")
            .replace({"CHF": "USDCHF", "CAD": "USDCAD", "JPY": "USDJPY"})
        )
    return df
