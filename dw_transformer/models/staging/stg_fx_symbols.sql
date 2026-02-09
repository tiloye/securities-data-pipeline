select
    -- {{ dbt_utils.generate_surrogate_key(['symbol']) }} as symbol_id,
    symbol,
    null as name,
    null as sector,
    null as industry,
    'FX' as asset_type,
    False as in_sp400,
    False as in_sp500,
    False as in_sp600,
    cast(null as date) as date_stamp
from (select distinct symbol from {{ source ("raw", "price_history_fx") }})