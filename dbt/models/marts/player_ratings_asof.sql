-- Grain: one row per player per match date (IPL and Full Member T20I matches), with every
-- rating as of the morning of that date. Wide form of int_player_ratings_long:
--   {skill}_{phase}_{stat}              shrunk rating per ball against all bowlers
--   bat_{phase}_vs_{pace|spin}_{stat}   the same, only balls faced against pace or spin
--   {skill}_{phase}_balls               decayed, competition-weighted balls behind it
--   bat_{phase}_vs_{pace|spin}_balls    and behind each split rating
-- Bowling bowl runs_per_ball x 6 is economy. source_match_date is the latest evidence used,
-- always before match_date.
{%- set balls_cols = [] %}
{%- for skill, vs in [('bat', 'all'), ('bat', 'pace'), ('bat', 'spin'), ('bowl', 'all')] %}
{%- for p in rating_phases() %}{% do balls_cols.append((skill, vs, p)) %}{% endfor %}
{%- endfor %}
select
    player_id::varchar as player_id,
    match_date::date as match_date,
    any_value(role)::varchar as role,
    any_value(bowl_share)::double as bowl_share,
    max(source_match_date)::date as source_match_date,
    any_value(prior_year)::integer as prior_year,
{%- for r in rating_series() %}
    max(rating) filter (where skill = '{{ r.skill }}' and phase = '{{ r.phase }}' and stat = '{{ r.stat }}' and vs = '{{ r.vs }}')::double
        as {{ rating_column(r.skill, r.phase, r.vs, r.stat) }},
{%- endfor %}
{%- for skill, vs, p in balls_cols %}
    max(balls) filter (where skill = '{{ skill }}' and phase = '{{ p }}' and stat = '{{ 'runs_per_ball' if skill == 'bat' else 'dot_rate' }}' and vs = '{{ vs }}')::double
        as {{ rating_column(skill, p, vs, 'balls') }}{{ ',' if not loop.last }}
{%- endfor %}
from {{ ref('int_player_ratings_long') }}
group by player_id, match_date
