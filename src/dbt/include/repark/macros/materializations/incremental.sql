{% materialization incremental, adapter='repark' %}
  {#
    G3-M1a: append + delete+insert only.
    delete+insert is two separate executes (not atomic — plan §1.5).
    Failure after delete leaves matching keys removed and nothing inserted.
    Injected failure (repark_fail_after_delete) also leaves the durable __dbt_tmp staging table.
  #}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation) -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh() or (existing_relation is not none and existing_relation.is_view)) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set raw_strategy = config.get('incremental_strategy') or 'append' -%}
  {# Loud refuse unsupported strategies before any SQL (M1.2). #}
  {% do repark_validate_incremental_strategy(raw_strategy) %}
  {# Normalize default → append after validation. #}
  {%- set incremental_strategy = 'append'
        if (raw_strategy | trim | lower) in ['append', 'default']
        else (raw_strategy | trim | lower) -%}
  {# Fail loud before staging work when delete+insert lacks unique_key. #}
  {% if incremental_strategy == 'delete+insert' and not unique_key %}
    {% do exceptions.raise_compiler_error(
      "dbt-repark incremental strategy 'delete+insert' requires config unique_key "
      "(one column name or list of column names)."
    ) %}
  {% endif %}

  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {%- set preexisting_temp_relation = load_cached_relation(temp_relation) -%}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}
  {{ drop_relation_if_exists(preexisting_temp_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [temp_relation] %}
  {% set need_swap = false %}

  {% if existing_relation is none %}
    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}
  {% elif full_refresh_mode %}
    {% call statement('main') -%}
      {{ create_table_as(False, intermediate_relation, sql) }}
    {%- endcall %}
    {% set need_swap = true %}
  {% else %}
    {# Staging is a durable Iceberg table (engine has no TEMP TABLE / TEMP VIEW). #}
    {% call statement('create_tmp') -%}
      {{ create_table_as(False, temp_relation, sql) }}
    {%- endcall %}

    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}

    {% if incremental_strategy == 'append' %}
      {% call statement('main') -%}
        {{ repark_get_incremental_append_sql(target_relation, temp_relation, dest_columns) }}
      {%- endcall %}
    {% elif incremental_strategy == 'delete+insert' %}
      {# unique_key validated above before staging #}

      {# Execute 1 of 2: delete matching keys (eager; not rolled back on later failure). #}
      {% set delete_sql = repark_get_incremental_delete_sql(
            target_relation, temp_relation, unique_key, incremental_predicates
        ) %}
      {% call statement('delete') -%}
        {{ delete_sql }}
      {%- endcall %}

      {# M1.3 failure injection: residual after delete is a real, documented state. #}
      {% if var('repark_fail_after_delete', false)
            or config.get('repark_fail_after_delete') %}
        {% do exceptions.raise_compiler_error(
          "repark_fail_after_delete: injected failure after delete, before insert "
          "(M1.3 residual test). Matching unique_key rows are already deleted; "
          "nothing from this batch was inserted. begin/commit/rollback are no-ops — "
          "this residual cannot be rolled back (plan §1.5)."
        ) %}
      {% endif %}

      {# Execute 2 of 2: insert batch rows. #}
      {% call statement('main') -%}
        {{ repark_get_incremental_insert_sql(target_relation, temp_relation, dest_columns) }}
      {%- endcall %}
    {% else %}
      {# Should be unreachable after repark_validate_incremental_strategy. #}
      {% do repark_validate_incremental_strategy(incremental_strategy) %}
    {% endif %}
  {% endif %}

  {% if need_swap %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
    {% do to_drop.append(backup_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {# commit is a documented no-op on repark — still call for dbt lifecycle #}
  {{ adapter.commit() }}

  {% for rel in to_drop %}
    {{ drop_relation_if_exists(rel) }}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
