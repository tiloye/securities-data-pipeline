with base_ as (
    select
        cast("date" as date) as date,
        {{ dbt_utils.generate_surrogate_key(['symbol']) }} as symbol_key,
        round(cast(open as decimal), 2) as open,
        round(cast(high as decimal), 2) as high,
        round(cast(low as decimal), 2) as low,
        round(cast(close as decimal), 2) as close,
        cast(volume as bigint) as volume
    from {{ source("raw", "price_history_sp_stocks") }}
),
 ffilled as (
    select
        "date",
        symbol_key,
        {{ ffill_candles('symbol_key') }}
    from base_
 )

 select * from ffilled