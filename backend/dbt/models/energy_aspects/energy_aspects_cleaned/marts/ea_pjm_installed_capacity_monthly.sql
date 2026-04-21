{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_ea_pjm_installed_capacity_monthly') }}
