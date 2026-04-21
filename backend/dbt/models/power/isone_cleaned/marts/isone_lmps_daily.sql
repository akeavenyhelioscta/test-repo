{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_isone_lmps_daily') }}
