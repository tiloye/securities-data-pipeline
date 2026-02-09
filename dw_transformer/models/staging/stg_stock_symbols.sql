select
    -- {{ dbt_utils.generate_surrogate_key(['symbol']) }} as symbol_id,
    symbol,
    name,
    sector,
    industry,
    'Stock' as asset_type,
    False as in_sp400,
    False as in_sp500,
    False as in_sp600,
    cast(date_stamp as date) as date_stamp
from {{ source("raw", "symbols_sp_stocks") }}