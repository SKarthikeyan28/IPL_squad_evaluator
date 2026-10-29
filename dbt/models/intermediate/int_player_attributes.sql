-- One row per player per attribute snapshot: the Wikidata/DBpedia values with the
-- hand-checked overrides (config/reference/player_attribute_overrides.csv) applied, and
-- nationality resolved through a fallback chain:
--   override -> Wikidata country for sport -> Wikipedia infobox country
--   -> the player's latest T20I team -> Wikidata citizenship
-- A source that lists several countries is skipped, not guessed between. A player with
-- overrides but no snapshot (not on Wikidata) gets a row dated by the override. Only
-- players who appear in a match get rows, so the history always joins to dim_player.
with snapshots as (
    select * from {{ ref('stg_silver__player_attributes') }}
),

overrides as (
    select
        player_id,
        max(value) filter (where field = 'date_of_birth') as date_of_birth,
        max(value) filter (where field = 'nationality') as nationality,
        max(value) filter (where field = 'batting_hand') as batting_hand,
        max(value) filter (where field = 'bowling_kind') as bowling_kind,
        max(value) filter (where field = 'bowling_arm') as bowling_arm,
        max(value) filter (where field = 'is_keeper') as is_keeper,
        string_agg(field, '|' order by field) as override_fields,
        min(retrieved_at) as first_retrieved_at
    from {{ ref('player_attribute_overrides') }}
    group by player_id
),

spine as (
    select player_id, retrieved_at from snapshots
    union
    select player_id, first_retrieved_at::timestamp
    from overrides
    where player_id not in (select player_id from snapshots)
      and player_id in (select player_id from {{ ref('stg_silver__match_players') }})
),

t20i_teams as (
    select mp.player_id, arg_max(mp.team, m.match_date) as t20i_team
    from {{ ref('stg_silver__match_players') }} mp
    join {{ ref('stg_silver__matches') }} m using (match_id)
    where m.competition = 't20i'
    group by mp.player_id
),

resolved as (
    select
        sp.player_id,
        sp.retrieved_at,
        s.wikidata_id,
        coalesce(o.date_of_birth::date, s.date_of_birth) as date_of_birth,
        case when s.sport_country not like '%|%' then s.sport_country end as sport_country,
        s.wikipedia_country,
        t.t20i_team,
        case when s.citizenship not like '%|%' then s.citizenship end as citizenship,
        o.nationality as nationality_override,
        coalesce(o.batting_hand, s.batting_hand) as batting_hand,
        coalesce(o.bowling_kind, s.bowling_kind) as bowling_kind,
        coalesce(o.bowling_arm, s.bowling_arm) as bowling_arm,
        coalesce(o.is_keeper::boolean, s.is_keeper, false) as is_keeper,
        s.bowling_style_raw,
        s.wikipedia_fields,
        o.override_fields
    from spine sp
    left join snapshots s using (player_id, retrieved_at)
    left join overrides o using (player_id)
    left join t20i_teams t using (player_id)
)

select
    player_id,
    retrieved_at,
    wikidata_id,
    date_of_birth,
    coalesce(nationality_override, sport_country, wikipedia_country, t20i_team, citizenship)
        as nationality,
    case
        when nationality_override is not null then 'override'
        when sport_country is not null then 'wikidata_sport_country'
        when wikipedia_country is not null then 'wikipedia_country'
        when t20i_team is not null then 't20i_team'
        when citizenship is not null then 'wikidata_citizenship'
    end as nationality_source,
    batting_hand,
    bowling_kind,
    bowling_arm,
    is_keeper,
    bowling_style_raw,
    wikipedia_fields,
    override_fields
from resolved
