{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_ercot_lmps_hourly') }}
