-- repark adapter macros (Spark-door emission; intersection-preferring capabilities)

{% macro repark__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create namespace if not exists {{ relation.database }}.{{ relation.schema }}
  {%- endcall -%}
{% endmacro %}

{% macro repark__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop namespace if exists {{ relation.database }}.{{ relation.schema }} cascade
  {%- endcall -%}
{% endmacro %}

{% macro repark__create_table_as(temporary, relation, sql) -%}
  {# Engine has no TEMP TABLE / TEMP VIEW. Staging uses durable Iceberg tables with a
     dbt temp/intermediate suffix and temporary=False (materializations drop them).
     temporary=True remains a loud refuse (M0 pin) — not a silent TEMP TABLE. #}
  {%- if temporary -%}
    {{ exceptions.raise_compiler_error(
      "dbt-repark does not support temporary tables (engine has no TEMP TABLE / TEMP VIEW). "
      "Use ephemeral models, or durable staging via create_table_as(False, temp_relation, …) "
      "with a dbt temp suffix (incremental materialization does this)."
    ) }}
  {%- endif -%}
  create or replace table {{ relation }}
  using iceberg
  as
  {{ sql }}
{%- endmacro %}

{% macro repark__drop_relation(relation) -%}
  {% call statement('drop_relation') -%}
    drop table if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro repark__rename_relation(from_relation, to_relation) -%}
  {# Engine requires a three-part catalog.namespace.table target name (bare identifier fails). #}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}

{% macro repark__truncate_relation(relation) -%}
  {{ exceptions.raise_compiler_error(
    "TRUNCATE is not supported on repark (engine refuse). Use CREATE OR REPLACE TABLE … AS SELECT or DELETE."
  ) }}
{% endmacro %}

{# list_schemas / list_relations / get_columns are implemented in Python (ReparkAdapter). #}

{% macro repark__list_schemas(database) -%}
  {% set res = adapter.list_schemas_via_catalog(database) %}
  {# Return a 1-column table-like list for SQLAdapter.list_schemas row[0] iteration. #}
  {% set rows = [] %}
  {% for name in res %}
    {% do rows.append([name]) %}
  {% endfor %}
  {{ return(rows) }}
{% endmacro %}

{% macro repark__check_schema_exists(information_schema, schema) -%}
  {% set schemas = adapter.list_schemas_via_catalog(information_schema.database) %}
  {% set n = 0 %}
  {% for s in schemas %}
    {% if s | lower == schema | lower %}
      {% set n = 1 %}
    {% endif %}
  {% endfor %}
  {{ return([[n]]) }}
{% endmacro %}

{% macro repark__list_relations_without_caching(schema_relation) -%}
  {# Prefer Python path; this macro is a safety net returning empty. #}
  {{ return([]) }}
{% endmacro %}

{% macro repark__get_columns_in_relation(relation) -%}
  {# Prefer Python get_columns_in_relation; unused when override is live. #}
  {{ return([]) }}
{% endmacro %}

{% macro repark__current_timestamp() -%}
  current_timestamp()
{%- endmacro %}
