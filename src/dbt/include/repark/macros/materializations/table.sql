{% materialization table, adapter='repark' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# Single-statement CREATE OR REPLACE path preferred when no rename dance needed.
     Use intermediate + rename when replacing to reduce window without the table. #}
  {% if existing_relation is none %}
    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}
  {% else %}
    {% call statement('main') -%}
      {{ create_table_as(False, intermediate_relation, sql) }}
    {%- endcall %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {# commit is a documented no-op on repark — still call for dbt lifecycle #}
  {{ adapter.commit() }}
  {{ drop_relation_if_exists(backup_relation) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
