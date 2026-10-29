-- SCD Type 2: one row per version of a player's attributes. A new version starts when a
-- tracked attribute changes between snapshots; snapshots that change nothing are folded
-- into the version before them. valid_from is when the pipeline first saw the version
-- (the snapshot time), valid_to when it was replaced, null while current.
-- Built from every raw snapshot, so the history is rebuilt from raw files, not kept as
-- state that a lost database would take with it (unlike a dbt snapshot).
with attributes as (
    select
        *,
        md5(concat_ws('|',
            coalesce(date_of_birth::varchar, '~'), coalesce(nationality, '~'),
            coalesce(batting_hand, '~'), coalesce(bowling_kind, '~'),
            coalesce(bowling_arm, '~'), is_keeper::varchar
        )) as attributes_hash
    from {{ ref('int_player_attributes') }}
),

changes as (
    select *
    from attributes
    qualify attributes_hash is distinct from
        lag(attributes_hash) over (partition by player_id order by retrieved_at)
),

versions as (
    select
        *,
        retrieved_at as valid_from,
        lead(retrieved_at) over (partition by player_id order by retrieved_at) as valid_to
    from changes
)

select
    (player_id || '@' || strftime(valid_from, '%Y%m%dT%H%M%S'))::varchar as player_version_key,
    player_id::varchar as player_id,
    date_of_birth::date as date_of_birth,
    nationality::varchar as nationality,
    nationality_source::varchar as nationality_source,
    batting_hand::varchar as batting_hand,
    bowling_kind::varchar as bowling_kind,
    bowling_arm::varchar as bowling_arm,
    is_keeper::boolean as is_keeper,
    bowling_style_raw::varchar as bowling_style_raw,
    wikipedia_fields::varchar as wikipedia_fields,
    override_fields::varchar as override_fields,
    valid_from::timestamp as valid_from,
    valid_to::timestamp as valid_to,
    (valid_to is null)::boolean as is_current
from versions
