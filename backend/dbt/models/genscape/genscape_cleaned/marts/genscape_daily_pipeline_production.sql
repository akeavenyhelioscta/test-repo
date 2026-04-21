{{
  config(
    materialized='view'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

SELECT * FROM {{ ref('staging_v2_daily_pipeline_production') }}
