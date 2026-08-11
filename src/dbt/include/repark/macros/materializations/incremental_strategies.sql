{#
  Incremental strategy helpers for dbt-repark (G3-M1a + G3-M1b).
  Supported: append, delete+insert, insert_overwrite (partitioned, dynamic semantics).
  Unsupported (loud refuse): merge (G3-M2), microbatch/other.
#}

{% macro repark_normalize_partition_by(partition_by) -%}
  {# Return a list of identity partition column names, or none. #}
  {%- if partition_by is none -%}
    {{ return(none) }}
  {%- elif partition_by is string -%}
    {%- set s = partition_by | trim -%}
    {%- if s == '' -%}
      {{ return(none) }}
    {%- else -%}
      {{ return([s]) }}
    {%- endif -%}
  {%- else -%}
    {# list / sequence of column names #}
    {%- set out = [] -%}
    {%- for c in partition_by -%}
      {%- do out.append(c) -%}
    {%- endfor -%}
    {%- if out | length == 0 -%}
      {{ return(none) }}
    {%- else -%}
      {{ return(out) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}


{% macro repark_partitioned_by_clause(partition_by=none) -%}
  {# Emit Spark-door PARTITIONED BY (identity cols) when configured. #}
  {%- set cols = repark_normalize_partition_by(
        partition_by if partition_by is not none else config.get('partition_by')
      ) -%}
  {%- if cols is not none -%}
    partitioned by (
    {%- for c in cols -%}
      {{ c }}{% if not loop.last %}, {% endif %}
    {%- endfor -%}
    )
  {%- endif -%}
{%- endmacro %}


{% macro repark_validate_incremental_strategy(strategy, partition_by=none) -%}
  {%- set s = (strategy or 'append') | trim | lower -%}
  {%- if s in ['append', 'default'] -%}
    {# default maps to append #}
  {%- elif s == 'delete+insert' -%}
    {# ok #}
  {%- elif s == 'insert_overwrite' -%}
    {# Requires partition_by — refuse whole-table overwrite-all (A10 footgun). #}
    {%- set pcols = repark_normalize_partition_by(
          partition_by if partition_by is not none else config.get('partition_by')
        ) -%}
    {%- if pcols is none -%}
      {% do exceptions.raise_compiler_error(
        "dbt-repark refuses incremental_strategy='insert_overwrite' without config "
        "partition_by (one identity partition column name or list of names). "
        "Whole-table INSERT OVERWRITE is an overwrite-all footgun; this adapter only "
        "offers dynamic partition overwrite (overwrite partitions present in the batch, "
        "leave other partitions alone — dbt-spark DYNAMIC semantics). "
        "Set partition_by, or use incremental_strategy='delete+insert' (with unique_key) "
        "or full-refresh."
      ) %}
    {%- endif -%}
  {%- elif s == 'merge' -%}
    {% do exceptions.raise_compiler_error(
      "dbt-repark refuses incremental_strategy='merge' in G3-M1a/M1b. "
      "MERGE INTO upsert is scheduled for G3-M2 (not M1). "
      "Use incremental_strategy='append', 'delete+insert', or 'insert_overwrite'."
    ) %}
  {%- elif s == 'microbatch' -%}
    {% do exceptions.raise_compiler_error(
      "dbt-repark refuses incremental_strategy='microbatch': not supported. "
      "Supported strategies: append, delete+insert, insert_overwrite."
    ) %}
  {%- else -%}
    {% do exceptions.raise_compiler_error(
      "dbt-repark refuses incremental_strategy='" ~ strategy ~ "': not supported. "
      "Supported strategies: append, delete+insert, insert_overwrite. "
      "(merge → G3-M2.)"
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


{% macro repark_get_incremental_insert_overwrite_sql(
      target_relation, temp_relation, dest_columns, partition_by
) -%}
  {#
    Dynamic partition overwrite via the Spark-door INSERT OVERWRITE.

    Engine INSERT OVERWRITE is static whole-table replace (overwrite_by_row_filter(AlwaysTrue);
    PARTITION (…) clause refused; partitionOverwriteMode DYNAMIC not implemented). To honor
    dbt-spark DYNAMIC semantics — overwrite only partitions present in the batch, leave others
    alone — the adapter composes a single INSERT OVERWRITE whose source is:

      (batch rows) UNION ALL (existing target rows whose partition keys are absent from batch)

    That still uses the real INSERT OVERWRITE door (one execute). Residual honesty: this is
    not engine-native dynamic partition overwrite; it materializes kept partitions through the
    overwrite source. Staging remains a durable __dbt_tmp Iceberg table (engine has no TEMP).
  #}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) -%}
  {%- set pcols = repark_normalize_partition_by(partition_by) -%}
  {%- set predicates = [] -%}
  {%- for key in pcols -%}
    {%- do predicates.append(
          'DBT_INTERNAL_DEST.' ~ adapter.quote(key)
          ~ ' = DBT_INTERNAL_SOURCE.' ~ adapter.quote(key)
        ) -%}
  {%- endfor -%}

  insert overwrite {{ target_relation }} ({{ dest_cols_csv }})
  select {{ dest_cols_csv }}
  from {{ temp_relation }}
  union all
  select {{ dest_cols_csv }}
  from {{ target_relation }} as DBT_INTERNAL_DEST
  where not exists (
    select 1
    from {{ temp_relation }} as DBT_INTERNAL_SOURCE
    where {{ predicates | join(' and ') }}
  )
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

    incremental_predicates / predicates: optional extra ON-clause fragments plumbed through
    (same shape as stock dbt delete+insert). Accepted and appended here; no dedicated e2e
    pin in M1a — exercise or gate further in G3-M2 if productized.
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
  {# Engine refuses multi-statement in one execute. delete+insert is intentionally
     two statement() calls inside materialization incremental (adapter='repark').
     Never emit delete;insert as a single build_sql string. #}
  {% do exceptions.raise_compiler_error(
    "dbt-repark delete+insert cannot run as a single SQL string (engine: one statement "
    "per execute; strategy is two non-atomic executes). Use the repark incremental "
    "materialization (materialized='incremental', incremental_strategy='delete+insert')."
  ) %}
{% endmacro %}

{% macro repark__get_incremental_insert_overwrite_sql(arg_dict) %}
  {% do return(repark_get_incremental_insert_overwrite_sql(
      arg_dict["target_relation"],
      arg_dict["temp_relation"],
      arg_dict["dest_columns"],
      arg_dict.get("partition_by", config.get("partition_by"))
  )) %}
{% endmacro %}
