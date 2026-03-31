select
    -- {{ dbt_utils.generate_surrogate_key(['symbol']) }} as symbol_id,
    symbol,
    name,
    sector,
    industry,
    'Stock' as asset_type,
    in_sp400,
    in_sp500,
    in_sp600,
    cast(date_stamp as date) as date_stamp
from {{ source("raw", "symbols_sp_stocks") }}