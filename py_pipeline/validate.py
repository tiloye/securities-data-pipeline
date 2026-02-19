from datetime import date

import pandera.pandas as pa
from pandas import DatetimeIndex


########## Raw Symbols Data Valiator ##########

raw_stock_symbols_schema = pa.DataFrameSchema(
    name="Raw stock symbols",
    columns={
        "Symbol": pa.Column(str),
        "Security": pa.Column(str),
        "GICS Sector": pa.Column(str, nullable=True),
        "GICS Sub-Industry": pa.Column(str, nullable=True),
        "in_sp400": pa.Column(pa.Object, nullable=True),
        "in_sp500": pa.Column(pa.Object, nullable=True),
        "in_sp600": pa.Column(pa.Object, nullable=True),
    },
)

raw_fx_symbols_schema = pa.DataFrameSchema(
    name="Raw FX symbols", columns={"Symbol": pa.Column(str)}
)


########## Transformed Symbols Data Valiator ##########

transformed_stock_symbols_schema = pa.DataFrameSchema(
    name="Transformed stock symbols",
    columns={
        "symbol": pa.Column(str),
        "name": pa.Column(str),
        "sector": pa.Column(str),
        "industry": pa.Column(str),
        "in_sp400": pa.Column(bool),
        "in_sp500": pa.Column(bool),
        "in_sp600": pa.Column(bool),
        "date_stamp": pa.Column(date),
    },
)

transformed_fx_symbols_schema = pa.DataFrameSchema(
    name="Transformed FX symbols",
    columns={"symbol": pa.Column(str)},
)


########## Raw Price Data Valiator ##########

raw_price_schema = pa.DataFrameSchema(
    name="Raw prices",
    columns={
        ("Open", ".+"): pa.Column(float, nullable=True, regex=True, coerce=True),
        ("High", ".+"): pa.Column(float, nullable=True, regex=True, coerce=True),
        ("Low", ".+"): pa.Column(float, nullable=True, regex=True, coerce=True),
        ("Close", ".+"): pa.Column(float, nullable=True, regex=True, coerce=True),
        ("Volume", ".+"): pa.Column("Int64", nullable=True, regex=True, coerce=True),
    },
    checks=[
        pa.Check(
            lambda df: list(df.columns.names) == ["Price", "Ticker"],
            name="check_column_index_names",
            error="Column level names do not match expected ['Price', 'Ticker']",
        ),
        pa.Check(
            lambda df: isinstance(df.index, DatetimeIndex),
            name="check_index_dtype",
            error="Index is not an instance of pandas DatetimeIndex",
        ),
    ],
)

transformed_price_schema = pa.DataFrameSchema(
    name="Transformed prices",
    columns={
        "date": pa.Column(date),
        "symbol": pa.Column(str),
        "close": pa.Column(float, nullable=True),
        "high": pa.Column(float, nullable=True),
        "low": pa.Column(float, nullable=True),
        "open": pa.Column(float, nullable=True),
        "volume": pa.Column("Int64", nullable=True),
    },
)
