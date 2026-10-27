-- One row per competition season. Rule flags come from config/reference/rule_changes.csv.
-- Purse, squad and overseas limits join here in Phase 5 (season rules research).
with seasons as (
    select
        competition,
        season,
        count(*) as matches,
        min(match_date) as first_match_date,
        max(match_date) as last_match_date
    from {{ ref('stg_silver__matches') }}
    group by competition, season
)

select
    (s.competition || '_' || s.season)::varchar as season_key,
    s.competition::varchar as competition,
    s.season::integer as season,
    s.matches::integer as matches,
    s.first_match_date::date as first_match_date,
    s.last_match_date::date as last_match_date,
    exists (
        select 1 from {{ ref('rule_changes') }} r
        where r.competition = s.competition
          and r.rule = 'impact_player'
          and s.season >= r.first_season
          and (r.last_season is null or s.season <= r.last_season)
    )::boolean as impact_player_rule
from seasons s
