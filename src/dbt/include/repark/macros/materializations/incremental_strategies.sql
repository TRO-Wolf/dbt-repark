{#
  Incremental strategy helpers for dbt-repark (G3-M1a).
  Supported: append, delete+insert.
  Unsupported (loud refuse): merge (G3-M2), insert_overwrite (OQ-5), microbatch/other.
#}

{% macro repark_validate_incremental_strategy(strategy) -%}
  {%- set s = (strategy or 'append') | trim | lower -%}
  {%- if s in ['append', 'default'] -%}
    {# default maps to append #}
  {%- elif s == 'delete+insert' -%}
    {# ok #}
  {%- elif s == 'merge' -%}
    {% do exceptions.raise_compiler_error(
      "dbt-repark refuses incremental_strategy='merge' in G3-M1a. "
      "MERGE INTO upsert is scheduled for G3-M2 (not M1). "
      "Use incremental_strategy='append' or 'delete+insert'."
    ) %}
  {%- elif s == 'insert_overwrite' -%}
    {% do exceptions.raise_compiler_error(
      "dbt-repark refuses incremental_strategy='insert_overwrite' (OQ-5 ruling: omit M0–M1; "
      "prioritized as Spark-only optional strategy for a later milestone / M3). "
      "Use incremental_strategy='append' or 'delete+insert'."
    ) %}
  {%- elif s == 'microbatch' -%}
    {% do exceptions.raise_compiler_error(
      "dbt-repark refuses incremental_strategy='microbatch': not supported. "
      "Supported strategies: append, delete+insert."
    ) %}
  {%- else -%}
    {% do exceptions.raise_compiler_error(
      "dbt-repark refuses incremental_strategy='" ~ strategy ~ "': not supported. "
      "Supported strategies: append, delete+insert. "
      "(merge → G3-M2; insert_overwrite → OQ-5 deferred.)"
    ) %}
  {%- endif -%}
{%- endmacro %}


{% macro repark_get_incremental_append_sql(target_relation, temp_relation, dest_columns) -%}
  {{ repark_get_incremental_insert_sql(target_relation, temp_relation, dest_columns) }}
{%- endmacro %}


{% macro repark_get_incremental_insert_sql(target_relation, temp_relation, dest_columns) -%}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) -%}
  insert into {{ target_relation }} ({{ dest_cols_csv }})
  select {{ dest_cols_csv }}
  from {{ temp_relation }}
{%- endmacro %}


{% macro repark_get_incremental_delete_sql(target_relation, temp_relation, unique_key, incremental_predicates=none) -%}
  {#
    Delete half of delete+insert (one eager execute).

    Engine note: DELETE … WHERE key IN (SELECT …) currently removes *all* rows on repark
    (incorrect). We therefore use MERGE … WHEN MATCHED THEN DELETE as a single-predicate
    *delete vehicle* only — this is NOT the G3-M2 merge incremental strategy
    (delete-only MERGE; insert is a separate execute, not combined upsert).

    Source side is SELECT DISTINCT on unique_key columns so duplicate batch keys do not
    trip MERGE_CARDINALITY_VIOLATION.
  #}
  {%- if unique_key is string -%}
    {%- set unique_key_list = [unique_key] -%}
  {%- else -%}
    {%- set unique_key_list = unique_key -%}
  {%- endif -%}

  {%- set key_cols = [] -%}
  {%- for key in unique_key_list -%}
    {%- do key_cols.append(adapter.quote(key)) -%}
  {%- endfor -%}
  {%- set key_cols_csv = key_cols | join(', ') -%}

  {%- set predicates = [] -%}
  {%- for key in unique_key_list -%}
    {%- do predicates.append(
          'DBT_INTERNAL_DEST.' ~ adapter.quote(key) ~ ' = DBT_INTERNAL_SOURCE.' ~ adapter.quote(key)
        ) -%}
  {%- endfor -%}
  {%- if incremental_predicates -%}
    {%- for predicate in incremental_predicates -%}
      {%- do predicates.append(predicate) -%}
    {%- endfor -%}
  {%- endif -%}

  merge into {{ target_relation }} as DBT_INTERNAL_DEST
  using (
    select distinct {{ key_cols_csv }}
    from {{ temp_relation }}
  ) as DBT_INTERNAL_SOURCE
  on {{ predicates | join(' and ') }}
  when matched then delete
{%- endmacro %}


{#
  Dispatch hooks used if default incremental materialization path is ever hit.
  Primary path is the repark incremental materialization above.
#}
{% macro repark__get_incremental_default_sql(arg_dict) %}
  {% do return(repark_get_incremental_append_sql(
      arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"]
  )) %}
{% endmacro %}

{% macro repark__get_incremental_append_sql(arg_dict) %}
  {% do return(repark_get_incremental_append_sql(
      arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"]
  )) %}
{% endmacro %}

{% macro repark__get_incremental_delete_insert_sql(arg_dict) %}
  {# Multi-statement body; prefer repark materialization which splits executes. #}
  {% set unique_key = arg_dict.get("unique_key") %}
  {% set delete_sql = repark_get_incremental_delete_sql(
        arg_dict["target_relation"],
        arg_dict["temp_relation"],
        unique_key,
        arg_dict.get("incremental_predicates"),
    ) %}
  {% set insert_sql = repark_get_incremental_insert_sql(
        arg_dict["target_relation"],
        arg_dict["temp_relation"],
        arg_dict["dest_columns"],
    ) %}
  {% do return(delete_sql ~ ';\n' ~ insert_sql) %}
{% endmacro %}
