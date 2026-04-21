{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_ea_pjm_load_model_monthly') }}
