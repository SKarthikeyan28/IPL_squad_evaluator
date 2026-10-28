-- In every IPL match, either both sides are neutral or one is home and the other away.
select match_id, list(home_status order by home_status) as statuses
from {{ ref('fact_team_match') }}
where competition = 'ipl'
group by match_id
having statuses not in (['away', 'home'], ['neutral', 'neutral'])
