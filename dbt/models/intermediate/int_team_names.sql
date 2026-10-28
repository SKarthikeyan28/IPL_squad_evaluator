-- Every team name in the data, mapped to one team_id.
-- IPL: the franchise (renames share an id). Internationals: a slug of the nation name.
with names as (
    select distinct competition, team1 as team_name from {{ ref('stg_silver__matches') }}
    union
    select distinct competition, team2 as team_name from {{ ref('stg_silver__matches') }}
)

select
    n.competition,
    n.team_name,
    case
        when n.competition = 'ipl' then a.franchise_id
        else 'nat_' || {{ slug('n.team_name') }}
    end as team_id
from names n
left join {{ ref('team_aliases') }} a
    on n.competition = 'ipl' and a.team_name = n.team_name
