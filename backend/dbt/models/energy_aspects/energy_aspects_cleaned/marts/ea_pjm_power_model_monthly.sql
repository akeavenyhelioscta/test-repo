{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_ea_pjm_power_model_monthly') }}
