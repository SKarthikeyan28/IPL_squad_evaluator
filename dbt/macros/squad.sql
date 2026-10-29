{# The numeric squad_features columns: everything but the keys and n_xi. One list, so the
   match_features differences and the completeness tests cannot drift from the mart. #}
{% macro squad_feature_names() -%}
    {%- set names = [] -%}
    {%- for group in ['top', 'mid', 'finish'] %}{% for stat in ['runs_per_ball', 'dismissal_rate'] -%}
        {%- do names.append(group ~ '_bat_' ~ stat) -%}
    {%- endfor %}{% endfor %}
    {%- for phase in rating_phases() %}{% for stat in ['runs_per_ball', 'wicket_rate'] -%}
        {%- do names.append(phase ~ '_bowl_' ~ stat) -%}
    {%- endfor %}{% endfor %}
    {%- for count in ['n_bowling_options', 'n_pace_options', 'n_spin_options', 'batting_depth', 'n_all_rounders', 'n_low_sample_players'] -%}
        {%- do names.append(count) -%}
    {%- endfor %}
    {{ return(names) }}
{%- endmacro %}
