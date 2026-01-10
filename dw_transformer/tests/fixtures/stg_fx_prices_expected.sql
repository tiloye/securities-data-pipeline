(
    select
    cast('2025-01-01' as date) as date,
    md5('P1') as symbol_key,
    round(cast(1.10 as decimal), 5) as open,
    round(cast(1.12 as decimal), 5) as high,
    round(cast(1.09 as decimal), 5) as low,
    round(cast(1.11 as decimal), 5) as close,
    cast(1000 as bigint) as volume
) union all
(
    select
    cast('2025-01-02' as date),
    md5('P1'),
    round(cast(1.11 as decimal), 5),
    round(cast(1.11 as decimal), 5),
    round(cast(1.11 as decimal), 5),
    round(cast(1.11 as decimal), 5),
    0
) union all
(
    select
    cast('2025-01-01' as date),
    md5('P2'),
    round(cast(1.20 as decimal), 5),
    round(cast(1.22 as decimal), 5),
    round(cast(1.19 as decimal), 5),
    round(cast(1.21 as decimal), 5),
    cast(1000 as bigint)
) union all
(
    select
    cast('2025-01-02' as date),
    md5('P2'),
    round(cast(1.21 as decimal), 5),
    round(cast(1.21 as decimal), 5),
    round(cast(1.21 as decimal), 5),
    round(cast(1.21 as decimal), 5),
    0
)