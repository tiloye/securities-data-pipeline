{% macro ffill_candles(partition_column) -%}
    case
        when open is null then last_value(close) over (partition by {{ partition_column }} order by "date" rows between unbounded preceding and 1 preceding)
        else open
    end as open,
    case
        when high is null then last_value(close) over (partition by {{ partition_column }} order by "date" rows between unbounded preceding and 1 preceding)
        else high
    end as high,
    case
        when low is null then last_value(close) over (partition by {{ partition_column }} order by "date" rows between unbounded preceding and 1 preceding)
        else low
    end as low,
    case
        when close is null then last_value(close) over (partition by {{ partition_column }} order by "date" rows between unbounded preceding and 1 preceding)
        else close
    end as close,
    case when volume is null then 0 else volume end as volume
{%- endmacro %}