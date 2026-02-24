-- macros/get_region.sql
{% macro get_region(country_col) %}
  CASE
    WHEN LOWER(TRIM({{ country_col }})) IN ('south korea', 'korea', 'kr', 'japan', 'jp', 'china', 'cn', '대한민국') THEN 'Asia'
    WHEN LOWER(TRIM({{ country_col }})) IN ('usa', 'us', 'united states', 'canada', 'mexico') THEN 'Americas'
    WHEN LOWER(TRIM({{ country_col }})) IN ('germany', 'france', 'spain', 'uk', 'italy') THEN 'Europe'
    ELSE 'Other'
  END
{% endmacro %}