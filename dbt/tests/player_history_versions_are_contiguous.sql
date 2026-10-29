-- SCD2 integrity: each player has exactly one current version, versions don't overlap,
-- and each one ends exactly when the next begins.
with ordered as (
    select
        *,
        lead(valid_from) over (partition by player_id order by valid_from) as next_from
    from {{ ref('dim_player_history') }}
)

select player_id, 'not exactly one current version' as problem
from {{ ref('dim_player_history') }}
group by player_id
having count(*) filter (where is_current) != 1
union all
select player_id, 'valid_to does not meet the next valid_from'
from ordered
where valid_to is distinct from next_from or valid_to <= valid_from
