{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_caiso_lmps_daily') }}
