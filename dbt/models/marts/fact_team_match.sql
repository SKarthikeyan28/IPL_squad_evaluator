-- Grain: one row per team per match. Outcome, toss and batting order from the team's
-- side, plus IPL home status from the sourced reference data (null for T20Is: their
-- grounds are not curated yet).
with matches as (
    select * from {{ ref('stg_silver__matches') }}
),

first_innings as (
    select match_id, batting_team as batted_first
    from {{ ref('stg_silver__innings') }}
    where innings_number = 1
),

sides as (
    select *, team1 as team, team2 as opponent from matches
    union all
    select *, team2 as team, team1 as opponent from matches
)

select
    (s.match_id || '-' || t.team_id)::varchar as team_match_key,
    s.match_id::varchar as match_id,
    t.team_id::varchar as team_id,
    o.team_id::varchar as opponent_team_id,
    s.competition::varchar as competition,
    s.season::integer as season,
    (s.competition || '_' || s.season)::varchar as season_key,
    s.match_date::date as match_date,
    v.venue_id::varchar as venue_id,
    h.home_status::varchar as home_status,
    (s.toss_winner = s.team)::boolean as won_toss,
    (f.batted_first = s.team)::boolean as batted_first,
    case
        when s.result = 'no_result' then 'no_result'
        when s.winner = s.team then 'won'
        when s.winner = s.opponent then 'lost'
        else 'tied'
    end::varchar as result,
    s.decided_by_super_over::boolean as decided_by_super_over,
    s.win_method::varchar as win_method
from sides s
join {{ ref('int_team_names') }} t on t.competition = s.competition and t.team_name = s.team
join {{ ref('int_team_names') }} o on o.competition = s.competition and o.team_name = s.opponent
left join {{ ref('int_venue_names') }} v on v.venue_name = s.venue_name
left join {{ ref('int_match_home_status') }} h on h.match_id = s.match_id and h.team = s.team
left join first_innings f on f.match_id = s.match_id
