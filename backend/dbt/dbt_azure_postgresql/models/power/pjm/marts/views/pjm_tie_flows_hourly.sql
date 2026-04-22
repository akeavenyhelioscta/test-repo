{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_pjm_tie_flows_hourly') }}
