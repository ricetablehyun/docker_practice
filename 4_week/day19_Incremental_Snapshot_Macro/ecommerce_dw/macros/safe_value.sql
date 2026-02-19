{% macro safe_value(col, default_val) %}
    COALESCE({{ col }}, '{{ default_val }}')
{% endmacro %}


