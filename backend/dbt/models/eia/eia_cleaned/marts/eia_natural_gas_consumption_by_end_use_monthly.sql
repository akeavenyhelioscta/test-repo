{{
  config(
    materialized='view'
  )
}}

---------------------------
-- EIA NATURAL GAS CONSUMPTION BY END USE — MONTHLY
---------------------------

SELECT * FROM {{ ref('staging_v1_eia_ng_consumption_by_end_use_monthly') }}
ORDER BY year DESC, month DESC, area_name_standardized
