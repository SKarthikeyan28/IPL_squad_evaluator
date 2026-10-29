-- Grain: player x match date x skill x phase x stat x vs, for every player in an IPL or Full
-- Member T20I match. Everything is as of the morning of match_date: evidence strictly
-- before it, priors from before 1 January of its year.
--   rating = (x + prior_balls * prior_mean) / (n + prior_balls)
-- With no evidence (n = 0) the rating is the prior; with lots it is the player's own rate.
--
-- The prior of a pace or spin batting series is not the role's average against that kind
-- of bowler: it is the batter's own rating against all bowlers, scaled by how the role
-- scores against that kind relative to all bowlers,
--   prior_mean = overall_rating * series_prior_mean / overall_series_prior_mean
-- and it is worth rating_split_prior_multiplier times the series' prior strength. A
-- batter's rating against spin is mostly his overall skill plus a small deviation, so
-- shrinking it toward the role average discards what his other balls already tell us (see
-- reports/pacespin_split.md). rating_split_shrink_to: role switches this off. Where the
-- overall series' pooled average is 0 the scaling is undefined (0 / 0) and the series prior
-- is used.
-- series_prior_mean and series_prior_balls are always the role-level values.
with targets as (
    select distinct mp.player_id, m.match_date as asof_date
    from {{ ref('stg_silver__match_players') }} mp
    join {{ ref('stg_silver__matches') }} m using (match_id)
    where m.match_id in (
        select match_id
        from {{ ref('fact_delivery') }}
        where competition = 'ipl' or is_full_member_t20i
    )
),

stats as (
    {{ rating_stats_asof('targets') }}
),

roles as (
    {{ rating_roles('stats') }}
),

priors as (
    select * from {{ ref('int_rating_priors') }}
),

with_prior as (
    select
        s.*,
        r.role,
        r.bowl_share,
        coalesce(own.role, pooled.role) as prior_role,
        coalesce(own.prior_mean, pooled.prior_mean) as series_prior_mean,
        coalesce(own.prior_balls, pooled.prior_balls) as series_prior_balls
    from stats s
    join roles r using (player_id, asof_date)
    left join priors own
        on own.prior_year = year(s.asof_date) and own.role = r.role
        and own.skill = s.skill and own.phase = s.phase and own.stat = s.stat and own.vs = s.vs
    left join priors pooled
        on pooled.prior_year = year(s.asof_date) and pooled.role = 'any'
        and pooled.skill = s.skill and pooled.phase = s.phase and pooled.stat = s.stat
        and pooled.vs = s.vs
),

overall as (
    select
        player_id, asof_date, skill, phase, stat,
        series_prior_mean as overall_series_prior_mean,
        (x + series_prior_balls * series_prior_mean) / (n + series_prior_balls) as overall_rating
    from with_prior
    where vs = 'all'
),

effective as (
    select
        w.*,
        {%- if var('rating_split_shrink_to') == 'role' %}
        w.series_prior_mean as prior_mean,
        w.series_prior_balls as prior_balls
        {%- else %}
        case
            when w.vs = 'all' then w.series_prior_mean
            else coalesce(
                o.overall_rating * w.series_prior_mean / nullif(o.overall_series_prior_mean, 0),
                w.series_prior_mean
            )
        end as prior_mean,
        case
            when w.vs = 'all' then w.series_prior_balls
            else w.series_prior_balls * {{ var('rating_split_prior_multiplier') }}
        end as prior_balls
        {%- endif %}
    from with_prior w
    left join overall o using (player_id, asof_date, skill, phase, stat)
)

select
    player_id,
    asof_date as match_date,
    role,
    bowl_share,
    skill,
    phase,
    stat,
    kind,
    vs,
    n as balls,
    x / nullif(n, 0) as raw_rate,
    year(asof_date) as prior_year,
    prior_role,
    series_prior_mean,
    series_prior_balls,
    prior_mean,
    prior_balls,
    (x + prior_balls * prior_mean) / (n + prior_balls) as rating,
    source_match_date
from effective
