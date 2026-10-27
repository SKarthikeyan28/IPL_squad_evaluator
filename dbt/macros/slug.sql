{# Lowercase, non-alphanumerics to underscores, trimmed: "Papua New Guinea" -> papua_new_guinea #}
{% macro slug(expr) -%}
    trim(regexp_replace(lower(trim({{ expr }})), '[^a-z0-9]+', '_', 'g'), '_')
{%- endmacro %}
