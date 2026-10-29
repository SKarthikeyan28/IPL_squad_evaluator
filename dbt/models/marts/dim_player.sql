-- One row per person who appears in a match. Names come from the Cricsheet register;
-- a player missing from it (the register can lag a newly published match) falls back
-- to the most recent name on a scorecard and is flagged in_register = false, so the
-- gap is visible instead of breaking the build. Attribute columns are the player's
-- current version from dim_player_history (SCD Type 2), null when no source has them.
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
    a.t20i_matches::integer as t20i_matches,
    h.date_of_birth::date as date_of_birth,
    h.nationality::varchar as nationality,
    h.nationality_source::varchar as nationality_source,
    h.batting_hand::varchar as batting_hand,
    h.bowling_kind::varchar as bowling_kind,
    h.bowling_arm::varchar as bowling_arm,
    coalesce(h.is_keeper, false)::boolean as is_keeper
from appearances a
left join {{ ref('stg_silver__players') }} p using (player_id)
left join {{ ref('dim_player_history') }} h on h.player_id = a.player_id and h.is_current
