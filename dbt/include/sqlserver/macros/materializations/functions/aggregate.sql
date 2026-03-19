{% macro sqlserver__aggregate_function_sql(target_relation) %}

  {% do exceptions.raise_compiler_error(
      "SQL Server does not support user-defined aggregate functions via T-SQL. "
      ~ "Aggregate functions require CLR assemblies, which cannot be managed through dbt. "
      ~ "Consider using a scalar function with a window function instead."
  ) %}

{% endmacro %}
