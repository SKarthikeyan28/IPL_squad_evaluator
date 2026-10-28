{# The rated statistics. `kind` picks the empirical Bayes model: beta-binomial for a rate of
   0/1 outcomes, normal-normal for runs. Batting stats share balls faced; bowling stats share
   legal balls bowled, so `balls` is the same for every stat of one skill and phase. #}
{% macro rating_stats() -%}
    {{ return([
        {'skill': 'bat', 'stat': 'runs_per_ball', 'kind': 'normal'},
        {'skill': 'bat', 'stat': 'dismissal_rate', 'kind': 'beta'},
        {'skill': 'bowl', 'stat': 'runs_per_ball', 'kind': 'normal'},
        {'skill': 'bowl', 'stat': 'wicket_rate', 'kind': 'beta'},
        {'skill': 'bowl', 'stat': 'dot_rate', 'kind': 'beta'},
    ]) }}
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

{# Every (skill, phase, stat) series, as a relation #}
{% macro rating_stat_grid() -%}
    (values
    {%- for s in rating_stats() %}{% for p in rating_phases() %}
        ('{{ s.skill }}', '{{ p }}', '{{ s.stat }}', '{{ s.kind }}'){{ ',' if not loop.last }}
    {%- endfor %}{{ ',' if not loop.last }}{% endfor %}
    ) as g(skill, phase, stat, kind)
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
            sum(n) filter (where skill = 'bat' and stat = 'runs_per_ball') as bat_balls,
            sum(n) filter (where skill = 'bowl' and stat = 'dot_rate') as bowl_balls
        from {{ stats }}
        group by player_id, asof_date
    )
{%- endmacro %}
