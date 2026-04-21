{{
  config(
    materialized='incremental',
    unique_key=['datetime_beginning_utc', 'tie_flow_name'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['datetime_beginning_utc', 'tie_flow_name'], 'type': 'btree'},
      {'columns': ['datetime_beginning_utc'], 'type': 'btree'}
    ]
  )
}}

SELECT * FROM {{ ref('staging_v1_pjm_tie_flows_hourly') }}

{% if is_incremental() %}
WHERE datetime_beginning_utc >= (SELECT MAX(datetime_beginning_utc) - INTERVAL '10 days' FROM {{ this }})
{% endif %}
