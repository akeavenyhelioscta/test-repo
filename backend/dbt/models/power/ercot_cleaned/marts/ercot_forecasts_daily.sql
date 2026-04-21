{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_ercot_gridstatus_forecasts_daily') }}
