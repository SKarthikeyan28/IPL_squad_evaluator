-- Grain: player x match date x skill x phase x stat, for every player in an IPL or Full
-- Member T20I match. Everything is as of the morning of match_date: evidence strictly
-- before it, priors from before 1 January of its year.
--   rating = (x + prior_balls * prior_mean) / (n + prior_balls)
-- With no evidence (n = 0) the rating is the prior; with lots it is the player's own rate.
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
        coalesce(own.prior_mean, pooled.prior_mean) as prior_mean,
        coalesce(own.prior_balls, pooled.prior_balls) as prior_balls
    from stats s
    join roles r using (player_id, asof_date)
    left join priors own
        on own.prior_year = year(s.asof_date) and own.role = r.role
        and own.skill = s.skill and own.phase = s.phase and own.stat = s.stat
    left join priors pooled
        on pooled.prior_year = year(s.asof_date) and pooled.role = 'any'
        and pooled.skill = s.skill and pooled.phase = s.phase and pooled.stat = s.stat
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
    n as balls,
    x / nullif(n, 0) as raw_rate,
    year(asof_date) as prior_year,
    prior_role,
    prior_mean,
    prior_balls,
    (x + prior_balls * prior_mean) / (n + prior_balls) as rating,
    source_match_date
from with_prior
