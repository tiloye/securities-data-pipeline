select *
from {{ ref('stg_fx_symbols') }}
union all
select *
from {{ ref('stg_stock_symbols') }}