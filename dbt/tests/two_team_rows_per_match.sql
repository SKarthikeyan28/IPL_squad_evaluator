-- fact_team_match has exactly two rows per match, one for each side.
select match_id, count(*) as rows_for_match
from {{ ref('fact_team_match') }}
group by match_id
having rows_for_match != 2
