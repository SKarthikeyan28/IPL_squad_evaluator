-- Every venue name in the data, mapped to one venue_id.
-- Curated aliases (config/reference/venue_aliases.csv) cover every IPL ground, including
-- IPL grounds that also host T20Is. Other names get an id from their own name: T20I
-- grounds are not curated yet, so two spellings of one T20I ground stay separate.
with names as (
    select venue_name, min(city) as city
    from {{ ref('stg_silver__matches') }}
    group by venue_name
)

select
    n.venue_name,
    coalesce(a.venue_id, 'v_' || {{ slug('n.venue_name') }}) as venue_id,
    a.venue_id is not null as is_curated,
    n.city as city_in_data
from names n
left join {{ ref('venue_aliases') }} a on a.venue_name = n.venue_name
