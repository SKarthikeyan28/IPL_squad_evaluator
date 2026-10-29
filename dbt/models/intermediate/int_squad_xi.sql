-- Grain: one row per player in an IPL team's starting XI. The XI is is_starting_xi, so an
-- Impact Player or concussion substitute is never in it, whether or not they came on.
-- slot orders the XI by expected batting position (ties by player_id): slot 1 opens.
-- role and bowl_share are as of the morning of the match.
with xi as (
    select
        (mp.match_id || '-' || t.team_id)::varchar as team_match_key,
        mp.match_id,
        t.team_id,
        mp.player_id,
        m.match_date,
        m.season
    from {{ ref('stg_silver__match_players') }} mp
    join {{ ref('stg_silver__matches') }} m using (match_id)
    join {{ ref('int_team_names') }} t
        on t.competition = m.competition and t.team_name = mp.team
    where m.competition = 'ipl' and mp.is_starting_xi
),

players as (
    select distinct player_id, match_date, role, bowl_share
    from {{ ref('int_player_ratings_long') }}
)

select
    xi.*,
    p.role,
    p.bowl_share,
    e.expected_position,
    row_number() over (
        partition by xi.team_match_key order by e.expected_position, xi.player_id
    )::integer as slot
from xi
join players p using (player_id, match_date)
join {{ ref('int_expected_batting_position') }} e using (player_id, match_date)
