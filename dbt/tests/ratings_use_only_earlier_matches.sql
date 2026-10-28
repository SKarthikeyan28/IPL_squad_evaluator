-- Point in time: a rating uses only evidence from before its match date, and the prior it
-- shrinks toward only evidence from before 1 January of that year.
select 'rating' as model, player_id, match_date as used_for, source_match_date
from {{ ref('player_ratings_asof') }}
where source_match_date >= match_date
   or prior_year != year(match_date)
union all
select 'prior', role || '/' || skill || '/' || phase || '/' || stat,
    make_date(prior_year, 1, 1), source_match_date
from {{ ref('int_rating_priors') }}
where source_match_date >= make_date(prior_year, 1, 1)
