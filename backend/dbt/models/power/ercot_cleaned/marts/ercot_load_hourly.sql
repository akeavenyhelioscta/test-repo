{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('source_v1_ercot_load_hourly') }}
