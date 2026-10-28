select player_id, name as player_name, unique_name, key_cricinfo
from {{ source('silver_registers', 'players') }}
