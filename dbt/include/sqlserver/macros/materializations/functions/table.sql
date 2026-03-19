{% macro sqlserver__table_function_sql(target_relation) %}

CREATE OR ALTER FUNCTION {{ target_relation.render() }} (
    {{ sqlserver__formatted_scalar_function_args_sql() }}
)
RETURNS TABLE
{{ sqlserver__scalar_function_volatility_sql() }}
AS
RETURN (
    {{ model.compiled_code }}
)

{% endmacro %}
