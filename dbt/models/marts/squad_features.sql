-- Grain: one row per IPL team per match: the strength of the starting XI, from each
-- player's rating as of the morning of the match. Only the XI is read: never the Impact
-- Player, never the match's own batting order (slots come from expected position).
--   top      slots 1-3, powerplay batting      mid     slots 4-6, middle-overs batting
--   finish   slots 5-8, death batting (5 and 6 face both middle and death)
--   *_bowl   the five XI players with the highest share of balls bowled, by phase
--   bowling option: bowl_share at least rating_role_batter_max_share
--   batting_depth: XI players who are not specialist bowlers (share below
--     rating_role_bowler_min_share); n_low_sample_players: role 'unknown'
with xi as (
    select
        x.*,
        row_number() over (
            partition by x.team_match_key
            order by coalesce(x.bowl_share, 0) desc, x.slot
        ) as bowl_rank,
        coalesce(x.bowl_share, 0) as share,
        d.bowling_kind
    from {{ ref('int_squad_xi') }} x
    left join {{ ref('dim_player') }} d using (player_id)
)

select
    xi.team_match_key::varchar as team_match_key,
    any_value(xi.match_id)::varchar as match_id,
    any_value(xi.team_id)::varchar as team_id,
    any_value(xi.match_date)::date as match_date,
    any_value(xi.season)::integer as season,
    count(*)::integer as n_xi,
    {%- for group, lo, hi, phase in [('top', 1, 3, 'powerplay'), ('mid', 4, 6, 'middle'), ('finish', 5, 8, 'death')] %}
    {%- for stat in ['runs_per_ball', 'dismissal_rate'] %}
    avg(r.bat_{{ phase }}_{{ stat }}) filter (where xi.slot between {{ lo }} and {{ hi }})::double
        as {{ group }}_bat_{{ stat }},
    {%- endfor %}{% endfor %}
    {%- for phase in rating_phases() %}
    {%- for stat in ['runs_per_ball', 'wicket_rate'] %}
    avg(r.bowl_{{ phase }}_{{ stat }}) filter (where xi.bowl_rank <= 5)::double
        as {{ phase }}_bowl_{{ stat }},
    {%- endfor %}{% endfor %}
    count(*) filter (where xi.share >= {{ var('rating_role_batter_max_share') }})::integer
        as n_bowling_options,
    count(*) filter (
        where xi.share >= {{ var('rating_role_batter_max_share') }} and xi.bowling_kind = 'pace'
    )::integer as n_pace_options,
    count(*) filter (
        where xi.share >= {{ var('rating_role_batter_max_share') }} and xi.bowling_kind = 'spin'
    )::integer as n_spin_options,
    count(*) filter (where xi.share < {{ var('rating_role_bowler_min_share') }})::integer
        as batting_depth,
    count(*) filter (where xi.role = 'all_rounder')::integer as n_all_rounders,
    count(*) filter (where xi.role = 'unknown')::integer as n_low_sample_players
from xi
join {{ ref('player_ratings_asof') }} r using (player_id, match_date)
group by xi.team_match_key
