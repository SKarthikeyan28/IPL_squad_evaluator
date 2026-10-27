-- One row per person who appears in a match. Names come from the Cricsheet register;
-- a player missing from it (the register can lag a newly published match) falls back
-- to the most recent name on a scorecard and is flagged in_register = false, so the
-- gap is visible instead of breaking the build. Attributes (batting hand, bowling
-- style, role) arrive with the attribute pipeline as SCD Type 2 rows.
with appearances as (
    select
        mp.player_id,
        min(m.match_date) as first_match_date,
        max(m.match_date) as last_match_date,
        arg_max(mp.player_name, m.match_date) as latest_scorecard_name,
        count(distinct case when m.competition = 'ipl' then m.match_id end) as ipl_matches,
        count(distinct case when m.competition = 't20i' then m.match_id end) as t20i_matches
    from {{ ref('stg_silver__match_players') }} mp
    join {{ ref('stg_silver__matches') }} m using (match_id)
    group by mp.player_id
)

select
    a.player_id::varchar as player_id,
    coalesce(p.player_name, a.latest_scorecard_name)::varchar as player_name,
    coalesce(p.unique_name, a.latest_scorecard_name)::varchar as unique_name,
    p.key_cricinfo::varchar as key_cricinfo,
    (p.player_id is not null)::boolean as in_register,
    a.first_match_date::date as first_match_date,
    a.last_match_date::date as last_match_date,
    a.ipl_matches::integer as ipl_matches,
    a.t20i_matches::integer as t20i_matches
from appearances a
left join {{ ref('stg_silver__players') }} p using (player_id)
