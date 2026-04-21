{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_miso_lmps_hourly') }}
