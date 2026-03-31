with base_ as (
    select
        cast(date_stamp as date) as date_stamp,
        symbol,
        round(cast(open as decimal), 2) as open,
        round(cast(high as decimal), 2) as high,
        round(cast(low as decimal), 2) as low,
        round(cast(close as decimal), 2) as close,
        cast(volume as bigint) as volume
    from {{ source("raw", "price_history_sp_stocks") }}
),
 ffilled as (
    select
        date_stamp,
        symbol,
        {{ ffill_candles('symbol') }}
    from base_
 )

 select * from ffilled