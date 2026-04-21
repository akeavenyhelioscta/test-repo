{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('source_v1_ercot_outages_hourly') }}
