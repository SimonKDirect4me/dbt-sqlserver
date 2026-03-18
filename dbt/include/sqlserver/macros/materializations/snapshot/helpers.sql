{% macro sqlserver__create_columns(relation, columns) %}
  {% set column_list %}
    {% for column_entry in columns %}
      {{column_entry.name}} {{column_entry.data_type}}{{ ", " if not loop.last }}
    {% endfor %}
  {% endset %}

  {% set alter_sql %}
    ALTER TABLE {{ relation }}
    ADD {{ column_list }}
  {% endset %}

  {% set results = run_query(alter_sql) %}

{% endmacro %}

{% macro sqlserver__snapshot_staging_table(strategy, temp_snapshot_relation, target_relation) -%}

    {% set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() %}

    with snapshot_query as (
        select * from {{ temp_snapshot_relation }}
    ),
    snapshotted_data as (
        select *,
        {{ unique_key_fields(strategy.unique_key) }}
        from {{ target_relation }}
        where
        {% if config.get('dbt_valid_to_current') %}
            ( {{ columns.dbt_valid_to }} = {{ config.get('dbt_valid_to_current') }} or {{ columns.dbt_valid_to }} is null)
        {% else %}
            {{ columns.dbt_valid_to }} is null
        {% endif %}
    ),
    insertions_source_data as (
        select *,
        {{ unique_key_fields(strategy.unique_key) }},
        {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
        {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
        {{ get_dbt_valid_to_current(strategy, columns) }},
        {{ strategy.scd_id }} as {{ columns.dbt_scd_id }}
        from snapshot_query
    ),
    updates_source_data as (
        select *,
        {{ unique_key_fields(strategy.unique_key) }},
        {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
        {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
        {{ strategy.updated_at }} as {{ columns.dbt_valid_to }}
        from snapshot_query
    ),
    {%- if strategy.invalidate_hard_deletes %}
        deletes_source_data as (
            select *, {{ unique_key_fields(strategy.unique_key) }}
            from snapshot_query
        ),
    {% endif %}
    insertions as (
        select 'insert' as dbt_change_type, source_data.*
        from insertions_source_data as source_data
        left outer join snapshotted_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
            where {{ unique_key_is_null(strategy.unique_key, "snapshotted_data") }}
            or ({{ unique_key_is_not_null(strategy.unique_key, "snapshotted_data") }} and ({{ strategy.row_changed }}))
    ),
    updates as (
        select 'update' as dbt_change_type, source_data.*,
        snapshotted_data.{{ columns.dbt_scd_id }}
        from updates_source_data as source_data
        join snapshotted_data
        on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
        where ({{ strategy.row_changed }})
    )
    {%- if strategy.invalidate_hard_deletes %}
        ,
        deletes as (
            select 'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as {{ columns.dbt_valid_from }},
            {{ snapshot_get_time() }} as {{ columns.dbt_updated_at }},
            {{ snapshot_get_time() }} as {{ columns.dbt_valid_to }},
            snapshotted_data.{{ columns.dbt_scd_id }}
            from snapshotted_data
            left join deletes_source_data as source_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
            where {{ unique_key_is_null(strategy.unique_key, "source_data") }}
        )
    {%- endif %}
    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
        union all
        select * from deletes
    {%- endif %}

{%- endmacro %}

{% macro build_snapshot_staging_table(strategy, temp_snapshot_relation, target_relation) %}
    {% set temp_relation = make_temp_relation(target_relation) %}
    {{ adapter.drop_relation(temp_relation) }}

    {% set select = snapshot_staging_table(strategy, temp_snapshot_relation, target_relation) %}

    {% set tmp_tble_vw_relation = temp_relation.incorporate(path={"identifier": temp_relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}
    -- Dropping temp view relation if it exists
    {{ adapter.drop_relation(tmp_tble_vw_relation) }}

    {% call statement('build_snapshot_staging_relation') %}
        {{ get_create_table_as_sql(True, temp_relation, select) }}
    {% endcall %}

    -- Dropping temp view relation if it exists
    {{ adapter.drop_relation(tmp_tble_vw_relation) }}

    {% do return(temp_relation) %}
{% endmacro %}
