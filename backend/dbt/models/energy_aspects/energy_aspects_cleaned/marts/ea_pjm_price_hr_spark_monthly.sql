{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_ea_pjm_price_hr_spark_monthly') }}
