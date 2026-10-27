-- Thin view over silver: the partition column typed as an integer, nothing else changed.
select * replace (cast(season as integer) as season)
from {{ source('silver', 'replacements') }}
