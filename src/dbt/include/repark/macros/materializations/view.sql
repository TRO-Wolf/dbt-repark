{% materialization view, adapter='repark' %}
  {#
    M0.2 / OQ-4 refuse-now: durable Iceberg VIEW is not a product surface (G3-E2).
    SQLAdapter fallthrough must not create session-scoped CREATE VIEW.
    Silent downgrade to table is pre-ruled out.
  #}
  {% set msg %}
dbt-repark refuses materialization='view': repark has no durable Iceberg VIEW surface
(G3-E2 / engine gap). Session-scoped CREATE VIEW is not an acceptable dbt target.
Set models: +materialized: table (adapter default recommendation) or wait for engine views.
  {% endset %}
  {% do exceptions.raise_compiler_error(msg | trim) %}
{% endmaterialization %}
