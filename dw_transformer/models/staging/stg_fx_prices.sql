with base_ as (
    select
    cast("date" as date) as date,
    {{ dbt_utils.generate_surrogate_key(['symbol']) }} as symbol_key,
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
        symbol_key,
        {{ ffill_candles('symbol_key') }}
from base_
)

select *
from ffill