with prices as (
    select
        date_stamp,
        symbol,
        open,
        high,
        low,
        close,
        volume
    from {{ ref('stg_fx_prices') }}
    union all
    select
        date_stamp,
        symbol,
        open,
        high,
        low,
        close,
        volume
    from {{ ref('stg_stock_prices') }}
)

select *
from prices