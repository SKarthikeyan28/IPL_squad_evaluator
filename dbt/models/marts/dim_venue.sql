-- One row per ground. Curated IPL grounds carry their canonical name, city and country;
-- other grounds use the name and city as recorded by Cricsheet.
with ids as (
    select
        venue_id,
        bool_or(is_curated) as is_curated,
        min(venue_name) as first_name_in_data,
        min(city_in_data) as city_in_data
    from {{ ref('int_venue_names') }}
    group by venue_id
)

select
    i.venue_id::varchar as venue_id,
    coalesce(v.canonical_name, i.first_name_in_data)::varchar as venue_name,
    coalesce(v.city, i.city_in_data)::varchar as city,
    v.country::varchar as country,
    i.is_curated::boolean as is_curated
from ids i
left join {{ ref('venues') }} v using (venue_id)
