-- Like squad_features_are_complete: a null difference from squad_features_complete_from on
-- would silently drop that match from every model.
select team_match_key, season
from {{ ref('match_features') }}
where season >= {{ var('squad_features_complete_from') }}
  and (
    {%- for name in squad_feature_names() %}
    d_{{ name }} is null{{ ' or' if not loop.last }}
    {%- endfor %}
  )
