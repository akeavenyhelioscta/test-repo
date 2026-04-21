{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_nyiso_lmps_hourly') }}
