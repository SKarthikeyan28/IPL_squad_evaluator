-- Running sums of each player's series up to and including each evidence date, every
-- term scaled by exp(decay_rate * day). Multiplying by exp(-decay_rate * D) later decays
-- the whole history to date D in one step (see the rating_stats_asof macro). Squared
-- weights decay twice as fast, so n2 is scaled by exp(2 * decay_rate * day).
select
    player_id,
    match_date,
    skill,
    phase,
    stat,
    vs,
    sum(n * exp({{ decay_rate() }} * {{ rating_day('match_date') }})) over w as cum_n,
    sum(x * exp({{ decay_rate() }} * {{ rating_day('match_date') }})) over w as cum_x,
    sum(x2 * exp({{ decay_rate() }} * {{ rating_day('match_date') }})) over w as cum_x2,
    sum(n2 * exp(2 * {{ decay_rate() }} * {{ rating_day('match_date') }})) over w as cum_n2
from {{ ref('int_rating_evidence') }}
window w as (
    partition by player_id, skill, phase, stat, vs
    order by match_date
    rows between unbounded preceding and current row
)
