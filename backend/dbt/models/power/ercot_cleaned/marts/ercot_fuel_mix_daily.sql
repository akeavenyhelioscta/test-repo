{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_ercot_fuel_mix_daily') }}
