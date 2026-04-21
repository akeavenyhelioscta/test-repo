{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_spp_lmps_hourly') }}
