select
    {{ dbt_utils.generate_surrogate_key(['symbol']) }} as symbol_key,
    symbol,
    null as name,
    null as sector,
    null as industry,
    'FX' as asset_type
from (select distinct symbol from {{ source ("raw", "price_history_fx") }})