-- macros/get_region.sql

{% macro get_region(country_col) %}
  CASE
    WHEN {{ country_col }} IN ('Germany', 'France', 'Spain', 'UK') THEN 'Europe'
    WHEN {{ country_col }} IN ('USA', 'Canada', 'Mexico') THEN 'Americas'
    WHEN {{ country_col }} IN ('Japan', 'Korea', 'China') THEN 'Asia'
    ELSE 'Other'
  END
{% endmacro %}