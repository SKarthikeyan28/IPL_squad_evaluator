-- One row per team: IPL franchises (all names of a franchise share one id) and
-- national T20I sides.
with seasons as (
    select n.team_id, min(m.season) as first_season_in_data, max(m.season) as last_season_in_data
    from {{ ref('stg_silver__matches') }} m
    join {{ ref('int_team_names') }} n
        on n.competition = m.competition and n.team_name in (m.team1, m.team2)
    group by n.team_id
),

ipl as (
    select
        f.franchise_id as team_id,
        f.franchise_name as team_name,
        'ipl' as competition,
        false as is_full_member_nation
    from {{ ref('franchises') }} f
),

nations as (
    select distinct
        n.team_id,
        n.team_name,
        't20i' as competition,
        fm.team_name is not null as is_full_member_nation
    from {{ ref('int_team_names') }} n
    left join {{ ref('full_member_nations') }} fm on fm.team_name = n.team_name
    where n.competition = 't20i'
)

select
    t.team_id::varchar as team_id,
    t.team_name::varchar as team_name,
    t.competition::varchar as competition,
    t.is_full_member_nation::boolean as is_full_member_nation,
    s.first_season_in_data::integer as first_season_in_data,
    s.last_season_in_data::integer as last_season_in_data
from (select * from ipl union all select * from nations) t
left join seasons s using (team_id)
