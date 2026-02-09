with base_ as (
    select
    cast("date" as date) as date,
    symbol,
    case
        when symbol = 'USDJPY' then round(cast(open as decimal), 3)
        else round(cast(open as decimal), 5)
    end as open,
    case
        when symbol = 'USDJPY' then round(cast(high as decimal), 3)
        else round(cast(high as decimal), 5)
    end as high,
    case
        when symbol = 'USDJPY' then round(cast(low as decimal), 3)
        else round(cast(low as decimal), 5)
    end as low,
    case
        when symbol = 'USDJPY' then round(cast(close as decimal), 3)
        else round(cast(close as decimal), 5)
    end as close,
    cast(volume as bigint) as volume
from {{ source("raw", "price_history_fx") }}
),
 ffill as (
    select
        date,
        symbol,
        {{ ffill_candles('symbol') }}
    from base_
)

select *
from ffill