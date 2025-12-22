with prices as (
    select *
    from {{ ref('stg_fx_prices') }}
    union all
    select *
    from {{ ref('stg_stock_prices') }}
)

select *
from prices