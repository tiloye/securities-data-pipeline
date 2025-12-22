select
    {{ dbt_utils.generate_surrogate_key(['symbol']) }} as symbol_key,
    symbol,
    name,
    sector,
    industry,
    'Stock' as asset_type
from {{ source("raw", "symbols_sp_stocks") }}