{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='id',
) }}
-- Sample only: placeholder incremental model (memory/dev). No real ARNs/buckets.
select 1 as id, 'sample' as label
{% if is_incremental() %}
union all
select 2 as id, 'second_run' as label
{% endif %}
