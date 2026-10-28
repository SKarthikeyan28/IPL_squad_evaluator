-- Every silver delivery reaches the fact table exactly once: no row lost or duplicated
-- by the joins. Returns a row (a failure) only when the counts differ.
select
    (select count(*) from {{ ref('stg_silver__deliveries') }}) as silver_rows,
    (select count(*) from {{ ref('fact_delivery') }}) as fact_rows
where silver_rows != fact_rows
