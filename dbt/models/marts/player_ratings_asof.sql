-- Grain: one row per player per match date (IPL and Full Member T20I matches), with every
-- rating as of the morning of that date. Wide form of int_player_ratings_long:
--   {skill}_{phase}_{stat}  shrunk rating per ball (bowl runs_per_ball x 6 is economy)
--   {skill}_{phase}_balls   decayed, competition-weighted balls behind it
-- source_match_date is the latest evidence used, always before match_date.
select
    player_id::varchar as player_id,
    match_date::date as match_date,
    any_value(role)::varchar as role,
    any_value(bowl_share)::double as bowl_share,
    max(source_match_date)::date as source_match_date,
    any_value(prior_year)::integer as prior_year,
{%- for s in rating_stats() %}{% for p in rating_phases() %}
    max(rating) filter (where skill = '{{ s.skill }}' and phase = '{{ p }}' and stat = '{{ s.stat }}')::double
        as {{ s.skill }}_{{ p }}_{{ s.stat }},
{%- endfor %}{% endfor %}
{%- for skill, stat in [('bat', 'runs_per_ball'), ('bowl', 'dot_rate')] %}{% for p in rating_phases() %}
    max(balls) filter (where skill = '{{ skill }}' and phase = '{{ p }}' and stat = '{{ stat }}')::double
        as {{ skill }}_{{ p }}_balls{{ ',' if not loop.last or skill == 'bat' }}
{%- endfor %}{% endfor %}
from {{ ref('int_player_ratings_long') }}
group by player_id, match_date
