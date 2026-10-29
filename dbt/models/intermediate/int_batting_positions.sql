-- Grain: one row per batter per innings (super overs excluded): the batting position, 1 to
-- 11, by order of first appearance at the crease as striker or non-striker. Of an opening
-- pair the striker is 1 and the non-striker 2. A batter who retires and returns keeps the
-- position of their first appearance.
with appearances as (
    select
        match_id, innings_number, batter_id as player_id,
        over_number * 1000 + ball_in_over * 2 as order_key
    from {{ ref('stg_silver__deliveries') }}
    union all
    select
        match_id, innings_number, non_striker_id as player_id,
        over_number * 1000 + ball_in_over * 2 + 1 as order_key
    from {{ ref('stg_silver__deliveries') }}
),

first_seen as (
    select match_id, innings_number, player_id, min(order_key) as order_key
    from appearances
    group by all
)

select
    f.match_id,
    f.innings_number,
    f.player_id,
    i.batting_team as team,
    m.competition,
    m.match_date,
    row_number() over (
        partition by f.match_id, f.innings_number order by f.order_key
    )::integer as position
from first_seen f
join {{ ref('stg_silver__innings') }} i using (match_id, innings_number)
join {{ ref('stg_silver__matches') }} m using (match_id)
where not i.is_super_over
