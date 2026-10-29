-- Every override names a known field, a player in the warehouse, and a value its field
-- accepts; a typo would otherwise be silently ignored. The fixture build sets
-- overrides_require_known_players to false: it holds 15 matches, not the whole league.
select o.*
from {{ ref('player_attribute_overrides') }} o
left join {{ ref('dim_player') }} p using (player_id)
where (p.player_id is null and {{ var('overrides_require_known_players', true) }})
   or o.field not in ('date_of_birth', 'nationality', 'batting_hand', 'bowling_kind',
                      'bowling_arm', 'is_keeper')
   or (o.field in ('batting_hand', 'bowling_arm') and o.value not in ('left', 'right'))
   or (o.field = 'bowling_kind' and o.value not in ('pace', 'spin'))
   or (o.field = 'is_keeper' and o.value not in ('true', 'false'))
   or (o.field = 'date_of_birth' and try_cast(o.value as date) is null)
   or o.source is null or o.retrieved_at is null or o.hand_checked is null
