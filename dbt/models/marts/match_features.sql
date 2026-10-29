-- Grain: one row per team per decided IPL match (no-results excluded; a tie is decided by
-- its super over, so it has a winner). The model input for "does this team win?":
--   d_<feature>   the team's squad feature minus its opponent's
--   home_edge     +1 home, -1 away, 0 neutral ground (one shared home effect, not per team)
--   chase_edge    +1 if the team bats second, -1 if first: known at the toss, and it carries
--                 the measured edge. Who won the toss is deliberately not a feature.
--   flags         the same for both sides: decided_by_super_over, reduced_overs (D/L),
--                 impact_player_era (the 2023+ rule)
-- Each match appears twice, once from each side, with every d_ column and both edges negated
-- and `won` flipped, so a model trained on both can be made symmetric. Null differences are
-- the 2008-09 seasons the ratings cannot cover (see squad_features).
select
    t.team_match_key::varchar as team_match_key,
    t.match_id::varchar as match_id,
    t.team_id::varchar as team_id,
    t.opponent_team_id::varchar as opponent_team_id,
    t.season::integer as season,
    t.match_date::date as match_date,
    (t.result = 'won')::boolean as won,
    t.decided_by_super_over::boolean as decided_by_super_over,
    coalesce(t.win_method = 'D/L', false)::boolean as reduced_overs,
    s.impact_player_rule::boolean as impact_player_era,
    case t.home_status when 'home' then 1 when 'away' then -1 else 0 end::integer as home_edge,
    case when t.batted_first then -1 else 1 end::integer as chase_edge,
    {%- for name in squad_feature_names() %}
    (own.{{ name }} - other.{{ name }})::double as d_{{ name }}{{ ',' if not loop.last }}
    {%- endfor %}
from {{ ref('fact_team_match') }} t
join {{ ref('squad_features') }} own on own.team_match_key = t.team_match_key
join {{ ref('squad_features') }} other
    on other.match_id = t.match_id and other.team_id = t.opponent_team_id
join {{ ref('dim_season') }} s on s.season_key = t.season_key
where t.competition = 'ipl' and t.result in ('won', 'lost')
