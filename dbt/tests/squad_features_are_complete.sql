-- (From squad_features_complete_from, 2010 by default.) The ratings have no history to build priors from in 2008, and none for death overs in
-- 2009, so those seasons' features are null. From 2010 every feature must be present: a new
-- gap would silently drop matches from every model.
select team_match_key, season
from {{ ref('squad_features') }}
where season >= {{ var('squad_features_complete_from') }}
  and (
    {%- for column in adapter.get_columns_in_relation(ref('squad_features')) if column.name not in ['team_match_key', 'match_id', 'team_id', 'match_date', 'season', 'n_xi'] %}
    {{ column.name }} is null{{ ' or' if not loop.last }}
    {%- endfor %}
  )
