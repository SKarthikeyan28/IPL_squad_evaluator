-- Grain: one row per ball bowled (wides and no-balls included), super overs flagged.
-- Measures are the raw counts; derived flags follow standard scoring conventions:
--   * phase: overs 1-6 powerplay, 7-15 middle, 16-20 death (0-based over_number)
--   * runs_conceded: runs charged to the bowler (off the bat + wides + no-balls;
--     byes and leg-byes are not the bowler's)
--   * bowler_wicket: dismissals credited to the bowler (not run outs, retirements,
--     obstructing the field, handled the ball, hit the ball twice, timed out)
--   * batter_dismissed: the striker was out (a non-striker run out does not count)
with deliveries as (
    select * from {{ ref('stg_silver__deliveries') }}
),

matches as (
    select match_id, competition, match_date, team1, team2, venue_name
    from {{ ref('stg_silver__matches') }}
),

innings as (
    select match_id, innings_number, batting_team, bowling_team, is_super_over
    from {{ ref('stg_silver__innings') }}
),

wickets as (
    select
        match_id, innings_number, over_number, ball_in_over,
        bool_or(kind in ('bowled', 'caught', 'caught and bowled', 'lbw', 'stumped', 'hit wicket'))
            as bowler_wicket,
        list(player_out_id) filter (where kind not in ('retired hurt', 'retired not out'))
            as players_out
    from {{ ref('stg_silver__wickets') }}
    group by all
),

full_members as (
    select team_name, full_member_from from {{ ref('full_member_nations') }}
)

select
    (d.match_id || '-' || d.innings_number || '-' || d.over_number || '-' || d.ball_in_over)::varchar
        as delivery_key,
    d.match_id::varchar as match_id,
    d.innings_number::integer as innings_number,
    d.over_number::integer as over_number,
    d.ball_in_over::integer as ball_in_over,
    m.competition::varchar as competition,
    d.season::integer as season,
    (m.competition || '_' || d.season)::varchar as season_key,
    m.match_date::date as match_date,
    v.venue_id::varchar as venue_id,
    bt.team_id::varchar as batting_team_id,
    bw.team_id::varchar as bowling_team_id,
    d.batter_id::varchar as batter_id,
    d.non_striker_id::varchar as non_striker_id,
    d.bowler_id::varchar as bowler_id,
    case
        when i.is_super_over then 'super_over'
        when d.over_number < 6 then 'powerplay'
        when d.over_number < 15 then 'middle'
        else 'death'
    end::varchar as phase,
    i.is_super_over::boolean as is_super_over,
    d.runs_batter::integer as runs_batter,
    d.runs_extras::integer as runs_extras,
    d.runs_total::integer as runs_total,
    d.extras_wides::integer as extras_wides,
    d.extras_noballs::integer as extras_noballs,
    d.extras_byes::integer as extras_byes,
    d.extras_legbyes::integer as extras_legbyes,
    d.extras_penalty::integer as extras_penalty,
    (d.runs_batter + d.extras_wides + d.extras_noballs)::integer as runs_conceded,
    d.is_legal::boolean as is_legal,
    (d.is_legal and d.runs_total = 0)::boolean as is_dot,
    (d.runs_batter = 4 and not d.is_non_boundary)::boolean as is_four,
    (d.runs_batter = 6 and not d.is_non_boundary)::boolean as is_six,
    d.n_wickets::integer as n_wickets,
    coalesce(w.bowler_wicket, false)::boolean as bowler_wicket,
    coalesce(list_contains(w.players_out, d.batter_id), false)::boolean as batter_dismissed,
    (
        m.competition = 't20i'
        and fa.team_name is not null and (fa.full_member_from is null or m.match_date >= fa.full_member_from)
        and fb.team_name is not null and (fb.full_member_from is null or m.match_date >= fb.full_member_from)
    )::boolean as is_full_member_t20i
from deliveries d
join matches m using (match_id)
join innings i using (match_id, innings_number)
left join wickets w using (match_id, innings_number, over_number, ball_in_over)
left join {{ ref('int_venue_names') }} v on v.venue_name = m.venue_name
left join {{ ref('int_team_names') }} bt
    on bt.competition = m.competition and bt.team_name = i.batting_team
left join {{ ref('int_team_names') }} bw
    on bw.competition = m.competition and bw.team_name = i.bowling_team
left join full_members fa on fa.team_name = m.team1
left join full_members fb on fb.team_name = m.team2
