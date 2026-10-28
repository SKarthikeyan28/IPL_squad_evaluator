-- Grain: player x match date x skill x phase x stat. The day's balls (n), outcomes (x) and
-- squared outcomes (x2), each ball weighted by competition: IPL 1, Full Member T20I less.
-- n2 sums the squared weights, which sampling noise depends on (see int_rating_priors).
-- Batting balls faced exclude wides; bowling balls are legal deliveries, while runs and
-- wickets off wides and no-balls still count against the bowler.
with balls as (
    select
        *,
        case when competition = 't20i' then {{ var('rating_t20i_weight') }} else 1.0 end as w
    from {{ ref('fact_delivery') }}
    where not is_super_over
      and (competition = 'ipl' or is_full_member_t20i)
),

long as (
    select batter_id as player_id, match_date, 'bat' as skill, phase, 'runs_per_ball' as stat,
        w * (extras_wides = 0)::int as n, w * runs_batter as x, w * runs_batter ^ 2 as x2,
        w ^ 2 * (extras_wides = 0)::int as n2
    from balls
    union all
    select batter_id, match_date, 'bat', phase, 'dismissal_rate',
        w * (extras_wides = 0)::int, w * batter_dismissed::int, w * batter_dismissed::int,
        w ^ 2 * (extras_wides = 0)::int
    from balls
    union all
    select bowler_id, match_date, 'bowl', phase, 'runs_per_ball',
        w * is_legal::int, w * runs_conceded, w * runs_conceded ^ 2, w ^ 2 * is_legal::int
    from balls
    union all
    select bowler_id, match_date, 'bowl', phase, 'wicket_rate',
        w * is_legal::int, w * bowler_wicket::int, w * bowler_wicket::int, w ^ 2 * is_legal::int
    from balls
    union all
    select bowler_id, match_date, 'bowl', phase, 'dot_rate',
        w * is_legal::int, w * is_dot::int, w * is_dot::int, w ^ 2 * is_legal::int
    from balls
)

select player_id, match_date, skill, phase, stat, sum(n) as n, sum(x) as x, sum(x2) as x2,
    sum(n2) as n2
from long
group by all
