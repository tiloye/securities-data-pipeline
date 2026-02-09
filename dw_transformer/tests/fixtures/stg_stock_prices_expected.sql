(
    select
    cast('2025-01-01' as date) as date,
    'S1' as symbol,
    round(cast(100.00 as decimal), 2) as open,
    round(cast(102.05 as decimal), 2) as high,
    round(cast(99.98 as decimal), 2) as low,
    round(cast(100.01 as decimal), 2) as close,
    cast(1000 as bigint) as volume
) union all
(
    select
    cast('2025-01-02' as date),
    'S1',
    round(cast(100.01 as decimal), 2),
    round(cast(100.01 as decimal), 2),
    round(cast(100.01 as decimal), 2),
    round(cast(100.01 as decimal), 2),
    0
) union all
(
    select
    cast('2025-01-01' as date),
    'S2',
    round(cast(180.15 as decimal), 2),
    round(cast(189.25 as decimal), 2),
    round(cast(178.95 as decimal), 2),
    round(cast(179.80 as decimal), 2),
    cast(1000 as bigint)
) union all
(
    select
    cast('2025-01-02' as date),
    'S2',
    round(cast(179.80 as decimal), 2),
    round(cast(179.80 as decimal), 2),
    round(cast(179.80 as decimal), 2),
    round(cast(179.80 as decimal), 2),
    0
)