{# The rated statistics. `kind` picks the empirical Bayes model: beta-binomial for a rate of
   0/1 outcomes, normal-normal for runs. Batting stats share balls faced; bowling stats share
   legal balls bowled, so `balls` is the same for every stat of one skill and phase.
   `vs` is who the player faced: 'all' bowlers, or only 'pace' or 'spin' bowlers. Only
   batting is split; the pace and spin series are subsets of the 'all' series. #}
{% macro rating_stats() -%}
    {{ return([
        {'skill': 'bat', 'stat': 'runs_per_ball', 'kind': 'normal', 'vs': ['all', 'pace', 'spin']},
        {'skill': 'bat', 'stat': 'dismissal_rate', 'kind': 'beta', 'vs': ['all', 'pace', 'spin']},
        {'skill': 'bowl', 'stat': 'runs_per_ball', 'kind': 'normal', 'vs': ['all']},
        {'skill': 'bowl', 'stat': 'wicket_rate', 'kind': 'beta', 'vs': ['all']},
        {'skill': 'bowl', 'stat': 'dot_rate', 'kind': 'beta', 'vs': ['all']},
    ]) }}
{%- endmacro %}

{# Column name in player_ratings_asof: bat_death_runs_per_ball, bat_death_vs_spin_runs_per_ball #}
{% macro rating_column(skill, phase, vs, stat) -%}
    {{ skill }}_{{ phase }}{{ '' if vs == 'all' else '_vs_' ~ vs }}_{{ stat }}
{%- endmacro %}

{% macro rating_phases() -%}
    {{ return(['powerplay', 'middle', 'death']) }}
{%- endmacro %}

{# Exponential decay rate per day, from the half-life #}
{% macro decay_rate() -%}
    (ln(2) / {{ var('rating_half_life_days') }})
{%- endmacro %}

{# Days since the reference date: keeps exp(decay_rate * day) far from overflow #}
{% macro rating_day(date_expr) -%}
    date_diff('day', date '2005-01-01', {{ date_expr }})
{%- endmacro %}

{# Every (skill, phase, stat, vs) series as a flat list of dicts #}
{% macro rating_series() -%}
    {%- set out = [] -%}
    {%- for s in rating_stats() %}{% for p in rating_phases() %}{% for v in s.vs -%}
        {%- do out.append({'skill': s.skill, 'phase': p, 'stat': s.stat, 'kind': s.kind, 'vs': v}) -%}
    {%- endfor %}{% endfor %}{% endfor -%}
    {{ return(out) }}
{%- endmacro %}

{# The series as a relation #}
{% macro rating_stat_grid() -%}
    (values
    {%- for r in rating_series() %}
        ('{{ r.skill }}', '{{ r.phase }}', '{{ r.stat }}', '{{ r.kind }}', '{{ r.vs }}'){{ ',' if not loop.last }}
    {%- endfor %}
    ) as g(skill, phase, stat, kind, vs)
{%- endmacro %}

{# Each (player_id, asof_date) row of `targets` crossed with every series, holding the
   player's decayed sums from evidence dated strictly before asof_date.
   cumulative holds sums scaled by exp(decay_rate * day), so
       sum_i exp(-decay_rate * (D - day_i)) * x_i = exp(-decay_rate * D) * cumulative
   and the ASOF JOIN picks the last evidence date before D. #}
{% macro rating_stats_asof(targets) -%}
    select
        t.player_id,
        t.asof_date,
        g.skill,
        g.phase,
        g.stat,
        g.kind,
        g.vs,
        c.match_date as source_match_date,
        coalesce(c.cum_n, 0) * exp(-{{ decay_rate() }} * {{ rating_day('t.asof_date') }}) as n,
        coalesce(c.cum_x, 0) * exp(-{{ decay_rate() }} * {{ rating_day('t.asof_date') }}) as x,
        coalesce(c.cum_x2, 0) * exp(-{{ decay_rate() }} * {{ rating_day('t.asof_date') }}) as x2,
        coalesce(c.cum_n2, 0) * exp(-2 * {{ decay_rate() }} * {{ rating_day('t.asof_date') }}) as n2
    from {{ targets }} t
    cross join {{ rating_stat_grid() }}
    asof left join {{ ref('int_rating_cumulative') }} c
        on c.player_id = t.player_id
        and c.skill = g.skill
        and c.phase = g.phase
        and c.stat = g.stat
        and c.vs = g.vs
        and t.asof_date > c.match_date
{%- endmacro %}

{# One row per (player_id, asof_date) of a rating_stats_asof relation: decayed balls faced
   and bowled (all phases), the share bowled, and the role that share implies #}
{% macro rating_roles(stats) -%}
    select
        player_id,
        asof_date,
        bat_balls,
        bowl_balls,
        bowl_balls / nullif(bat_balls + bowl_balls, 0) as bowl_share,
        case
            when bat_balls + bowl_balls < {{ var('rating_min_role_balls') }} then 'unknown'
            when bowl_balls / (bat_balls + bowl_balls) < {{ var('rating_role_batter_max_share') }}
                then 'batter'
            when bowl_balls / (bat_balls + bowl_balls) > {{ var('rating_role_bowler_min_share') }}
                then 'bowler'
            else 'all_rounder'
        end as role
    from (
        select
            player_id,
            asof_date,
            sum(n) filter (where skill = 'bat' and stat = 'runs_per_ball' and vs = 'all') as bat_balls,
            sum(n) filter (where skill = 'bowl' and stat = 'dot_rate' and vs = 'all') as bowl_balls
        from {{ stats }}
        group by player_id, asof_date
    )
{%- endmacro %}
