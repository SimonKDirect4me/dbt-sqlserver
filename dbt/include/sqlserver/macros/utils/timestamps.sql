{% macro sqlserver__current_timestamp() -%}
  CAST(SYSUTCDATETIME() AS DATETIME2(6))
{%- endmacro %}
