{% macro sqlserver__scalar_function_sql(target_relation) %}

CREATE OR ALTER FUNCTION {{ target_relation.render() }} (
    {{ sqlserver__formatted_scalar_function_args_sql() }}
)
RETURNS {{ model.returns.data_type }}
{{ sqlserver__scalar_function_volatility_sql() }}
AS
BEGIN
    RETURN (
        {{ model.compiled_code }}
    )
END

{% endmacro %}


{% macro sqlserver__formatted_scalar_function_args_sql() %}

  {%- for arg in model.arguments -%}
    @{{ arg.name }} {{ arg.data_type }}
    {%- if arg.default_value is not none %} = {{ arg.default_value }}{% endif %}
    {%- if not loop.last %},{{ "\n    " }}{% endif %}
  {%- endfor -%}

{% endmacro %}


{% macro sqlserver__scalar_function_volatility_sql() %}

  {%- set volatility = model.config.get('volatility') -%}
  {%- if volatility == 'deterministic' -%}
    WITH SCHEMABINDING
  {%- endif -%}

{% endmacro %}
