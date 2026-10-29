-- Empirical Bayes priors, one set per calendar year, estimated only from evidence dated
-- before 1 January of that year, so a prior never sees the matches it is used for.
--   prior_mean   the role's pooled rate, sum(x) / sum(n), over all its players: a mean
--                needs no large samples, and tail-enders' batting has its own mean
--   prior_balls  how many balls the prior is worth (k in (x + k * mean) / (n + k)), by
--                method of moments over players with rating_min_prior_balls in the phase:
--                  between = variance of their rates - the part sampling noise explains
--                  beta-binomial  k = mean * (1 - mean) / between - 1
--                  normal-normal  k = within-player per-ball variance / between
--                A rate from weighted balls has noise ball_var * n2 / n^2 (n2 = sum of
--                squared weights), not ball_var / n: decayed and T20I balls carry less.
-- Priors are per series, vs included: a batter's prior against spin is spin's role mean,
-- not the all-bowler mean. A pace or spin series too sparse to estimate its own prior
-- strength (spin at the death: few batters face 60 balls of it) borrows the strength of the
-- unsplit series, after trying its own earlier years; its mean is always its own. role = 'any' pools every role. A role with too few qualifying players borrows the pooled
-- prior_balls; a role with too few players at all is left out, and ratings use 'any'.
-- A year that can't estimate prior_balls carries the latest earlier year's forward (still
-- point in time); before the first estimable year there is no prior and no rating.
with cutoffs as (
    select distinct
        year(match_date) as prior_year,
        make_date(year(match_date), 1, 1) as asof_date
    from {{ ref('fact_delivery') }}
),

debuts as (
    select player_id, min(match_date) as first_date
    from {{ ref('int_rating_evidence') }}
    group by player_id
),

targets as (
    select d.player_id, c.asof_date
    from cutoffs c
    join debuts d on d.first_date < c.asof_date
),

stats as (
    {{ rating_stats_asof('targets') }}
),

roles as (
    {{ rating_roles('stats') }}
),

sample as (
    select year(s.asof_date) as prior_year, r.role, s.*
    from stats s
    join roles r using (player_id, asof_date)
    where s.n > 0
),

means as (
    select
        prior_year, coalesce(role, 'any') as role, skill, phase, stat, kind, vs,
        count(*) as n_players,
        sum(x) / sum(n) as prior_mean,
        max(source_match_date) as source_match_date
    from sample
    group by grouping sets (
        (prior_year, role, skill, phase, stat, kind, vs),
        (prior_year, skill, phase, stat, kind, vs)
    )
    having count(*) >= {{ var('rating_min_prior_players') }}
),

moments as (
    select
        prior_year, coalesce(role, 'any') as role, skill, phase, stat, kind, vs,
        sum(x) / sum(n) as mean,
        var_samp(x / n) as observed_var,
        avg(n2 / (n * n)) as mean_noise_factor,
        sum(x2 - x * x / n) / sum(n) as within_var
    from sample
    where n >= {{ var('rating_min_prior_balls') }}
    group by grouping sets (
        (prior_year, role, skill, phase, stat, kind, vs),
        (prior_year, skill, phase, stat, kind, vs)
    )
    having count(*) >= {{ var('rating_min_prior_players') }}
),

spread as (
    select
        *,
        observed_var - ball_var * mean_noise_factor as between_var
    from (
        select
            *,
            case kind when 'beta' then mean * (1 - mean) else within_var end as ball_var
        from moments
    )
),

strength as (
    select
        prior_year, role, skill, phase, stat, vs,
        case
            when between_var <= 0 then {{ var('rating_max_prior_balls') }}
            else least(
                greatest(
                    case kind
                        when 'beta' then mean * (1 - mean) / between_var - 1
                        else ball_var / between_var
                    end,
                    1
                ),
                {{ var('rating_max_prior_balls') }}
            )
        end as prior_balls
    from spread
)

select
    m.prior_year::integer as prior_year,
    m.role::varchar as role,
    m.skill::varchar as skill,
    m.phase::varchar as phase,
    m.stat::varchar as stat,
    m.kind::varchar as kind,
    m.vs::varchar as vs,
    m.n_players::integer as n_players,
    m.prior_mean::double as prior_mean,
    coalesce(
        own.prior_balls, pooled.prior_balls, own_unsplit.prior_balls, pooled_unsplit.prior_balls
    )::double as prior_balls,
    m.source_match_date::date as source_match_date
from means m
asof left join strength own
    on own.role = m.role and own.skill = m.skill and own.phase = m.phase and own.stat = m.stat
    and own.vs = m.vs
    and m.prior_year >= own.prior_year
asof left join strength pooled
    on pooled.role = 'any' and pooled.skill = m.skill and pooled.phase = m.phase
    and pooled.stat = m.stat and pooled.vs = m.vs
    and m.prior_year >= pooled.prior_year
asof left join strength own_unsplit
    on own_unsplit.role = m.role and own_unsplit.skill = m.skill
    and own_unsplit.phase = m.phase and own_unsplit.stat = m.stat and own_unsplit.vs = 'all'
    and m.prior_year >= own_unsplit.prior_year
asof left join strength pooled_unsplit
    on pooled_unsplit.role = 'any' and pooled_unsplit.skill = m.skill
    and pooled_unsplit.phase = m.phase and pooled_unsplit.stat = m.stat
    and pooled_unsplit.vs = 'all' and m.prior_year >= pooled_unsplit.prior_year
where coalesce(
    own.prior_balls, pooled.prior_balls, own_unsplit.prior_balls, pooled_unsplit.prior_balls
) is not null
