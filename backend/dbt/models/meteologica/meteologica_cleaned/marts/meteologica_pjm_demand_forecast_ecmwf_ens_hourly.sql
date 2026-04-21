{{
  config(
    materialized='view'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

SELECT * FROM {{ ref('staging_v1_meteo_pjm_demand_fcst_ecmwf_ens_hourly') }}
