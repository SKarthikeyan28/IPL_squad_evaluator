-- Grain: player x match date x skill x phase x stat x vs. The day's balls (n), outcomes (x)
-- and squared outcomes (x2), each ball weighted by competition: IPL 1, Full Member T20I less.
-- vs is who the batter faced: 'all' bowlers, or only 'pace' or 'spin' bowlers, by the
-- bowler's current bowling kind (dim_player_history has one version until the sources
-- change). A ball against a bowler of unknown kind is in 'all' only. Bowling is not split.
-- n2 sums the squared weights, which sampling noise depends on (see int_rating_priors).
-- Batting balls faced exclude wides; bowling balls are legal deliveries, while runs and
-- wickets off wides and no-balls still count against the bowler.
with bowler_kinds as (
    select player_id, bowling_kind
    from {{ ref('int_player_attributes') }}
    qualify row_number() over (partition by player_id order by retrieved_at desc) = 1
),

balls as (
    select
        d.*,
        k.bowling_kind as bowler_kind,
        case when d.competition = 't20i' then {{ var('rating_t20i_weight') }} else 1.0 end as w
    from {{ ref('fact_delivery') }} d
    left join bowler_kinds k on k.player_id = d.bowler_id
    where not d.is_super_over
      and (d.competition = 'ipl' or d.is_full_member_t20i)
),

long as (
    select batter_id as player_id, match_date, 'bat' as skill, phase, 'runs_per_ball' as stat,
        w * (extras_wides = 0)::int as n, w * runs_batter as x, w * runs_batter ^ 2 as x2,
        w ^ 2 * (extras_wides = 0)::int as n2, bowler_kind
    from balls
    union all
    select batter_id, match_date, 'bat', phase, 'dismissal_rate',
        w * (extras_wides = 0)::int, w * batter_dismissed::int, w * batter_dismissed::int,
        w ^ 2 * (extras_wides = 0)::int, bowler_kind
    from balls
    union all
    select bowler_id, match_date, 'bowl', phase, 'runs_per_ball',
        w * is_legal::int, w * runs_conceded, w * runs_conceded ^ 2, w ^ 2 * is_legal::int,
        bowler_kind
    from balls
    union all
    select bowler_id, match_date, 'bowl', phase, 'wicket_rate',
        w * is_legal::int, w * bowler_wicket::int, w * bowler_wicket::int, w ^ 2 * is_legal::int,
        bowler_kind
    from balls
    union all
    select bowler_id, match_date, 'bowl', phase, 'dot_rate',
        w * is_legal::int, w * is_dot::int, w * is_dot::int, w ^ 2 * is_legal::int,
        bowler_kind
    from balls
),

series as (
    select *, 'all' as vs from long
    union all
    select *, bowler_kind as vs from long where skill = 'bat' and bowler_kind is not null
)

select player_id, match_date, skill, phase, stat, vs, sum(n) as n, sum(x) as x,
    sum(x2) as x2, sum(n2) as n2
from series
group by all
