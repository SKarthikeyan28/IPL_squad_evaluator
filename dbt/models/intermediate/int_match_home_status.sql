-- Home status of each team in each IPL match: 'home', 'away' or 'neutral'.
-- The same rule as cricket_value/reference/match_home_status.sql (a test checks both
-- give identical results), now reading staged matches and the reference seeds.
--
-- 1. A match inside a neutral period (2009, 2020-2022, the 2014 UAE leg) is neutral.
-- 2. A team is at home when the venue is one of its franchise's home venues for that
--    season. Rows marked league_only apply to league matches only.
-- 3. Exactly one team at home -> home / away. Neither or both -> neutral.

with matches as (
    select * from {{ ref('stg_silver__matches') }} where competition = 'ipl'
),

sides as (
    select match_id, season, match_date, venue_name, event_stage, team1 as team, team2 as opponent
    from matches
    union all
    select match_id, season, match_date, venue_name, event_stage, team2 as team, team1 as opponent
    from matches
),

mapped as (
    select
        s.*,
        team_alias.franchise_id,
        opponent_alias.franchise_id as opponent_franchise_id,
        venue_alias.venue_id,
        exists (
            select 1 from {{ ref('neutral_periods') }} n
            where s.match_date between n.start_date and n.end_date
        ) as in_neutral_period
    from sides s
    left join {{ ref('team_aliases') }} team_alias on team_alias.team_name = s.team
    left join {{ ref('team_aliases') }} opponent_alias on opponent_alias.team_name = s.opponent
    left join {{ ref('venue_aliases') }} venue_alias on venue_alias.venue_name = s.venue_name
),

flagged as (
    select
        m.*,
        exists (
            select 1 from {{ ref('home_venues') }} h
            where h.franchise_id = m.franchise_id
              and h.venue_id = m.venue_id
              and m.season >= h.first_season
              and (h.last_season is null or m.season <= h.last_season)
              and (not h.league_only or m.event_stage is null)
        ) as team_at_home,
        exists (
            select 1 from {{ ref('home_venues') }} h
            where h.franchise_id = m.opponent_franchise_id
              and h.venue_id = m.venue_id
              and m.season >= h.first_season
              and (h.last_season is null or m.season <= h.last_season)
              and (not h.league_only or m.event_stage is null)
        ) as opponent_at_home
    from mapped m
)

select
    match_id,
    season,
    match_date,
    team,
    franchise_id,
    opponent_franchise_id,
    venue_id,
    case
        when in_neutral_period then 'neutral'
        when team_at_home and not opponent_at_home then 'home'
        when opponent_at_home and not team_at_home then 'away'
        else 'neutral'
    end as home_status,
    in_neutral_period,
    team_at_home,
    opponent_at_home
from flagged
