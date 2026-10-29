-- Grain: one row per IPL starting-XI player per match date: where we expect them to bat,
-- from innings strictly before that date only. The batting order of the match itself is
-- never read.
--   expected = (sum_w * position + k * default) / (sum_w + k)
-- sum_w * position is the exponentially decayed sum of the player's earlier IPL positions
-- (the same half-life as the ratings), and `default` is their role's average position over
-- IPL innings before 1 January of the match's year, so a debutant gets the role's typical
-- position. Roles are the ratings' roles on the date the position was observed.
-- decayed_innings is sum_w: 0 for a debutant.
with positions as (
    select p.player_id, p.match_date, p.position, r.role
    from {{ ref('int_batting_positions') }} p
    join (
        select distinct player_id, match_date, role from {{ ref('int_player_ratings_long') }}
    ) r using (player_id, match_date)
    where p.competition = 'ipl'
),

cumulative as (
    select
        player_id,
        match_date,
        sum(innings * exp({{ decay_rate() }} * {{ rating_day('match_date') }}))
            over (partition by player_id order by match_date) as cum_innings,
        sum(position_sum * exp({{ decay_rate() }} * {{ rating_day('match_date') }}))
            over (partition by player_id order by match_date) as cum_position
    from (
        select player_id, match_date, count(*) as innings, sum(position) as position_sum
        from positions
        group by all
    )
),

role_year as (
    select role, year(match_date) as year, count(*) as innings, sum(position) as position_sum
    from positions
    group by all
),

-- the role's average over every year up to and including `year`, usable from the next
role_defaults as (
    select
        role,
        year + 1 as usable_from_year,
        sum(position_sum) over (partition by role order by year)
            / sum(innings) over (partition by role order by year) as default_position
    from role_year
),

targets as (
    select distinct x.player_id, m.match_date as asof_date
    from {{ ref('stg_silver__match_players') }} x
    join {{ ref('stg_silver__matches') }} m using (match_id)
    where m.competition = 'ipl' and x.is_starting_xi
),

roles as (
    select distinct player_id, match_date as asof_date, role
    from {{ ref('int_player_ratings_long') }}
),

evidence as (
    select
        t.player_id,
        t.asof_date,
        r.role,
        year(t.asof_date) as year,
        coalesce(c.cum_innings, 0) * exp(-{{ decay_rate() }} * {{ rating_day('t.asof_date') }})
            as decayed_innings,
        coalesce(c.cum_position, 0) * exp(-{{ decay_rate() }} * {{ rating_day('t.asof_date') }})
            as decayed_position_sum
    from targets t
    join roles r using (player_id, asof_date)
    asof left join cumulative c
        on c.player_id = t.player_id and t.asof_date > c.match_date
)

select
    e.player_id,
    e.asof_date as match_date,
    e.role,
    e.decayed_innings,
    (
        e.decayed_position_sum
        + {{ var('batting_position_prior_matches') }} * coalesce(
            d.default_position,
            case e.role
                {%- for role, position in var('batting_position_fallback').items() %}
                when '{{ role }}' then {{ position }}
                {%- endfor %}
            end
        )
    ) / (e.decayed_innings + {{ var('batting_position_prior_matches') }}) as expected_position
from evidence e
asof left join role_defaults d
    on d.role = e.role and e.year >= d.usable_from_year
