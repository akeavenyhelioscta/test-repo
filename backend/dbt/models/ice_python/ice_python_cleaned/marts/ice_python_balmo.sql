{{
  config(
    materialized='view'
  )
}}

---------------------------
-- ICE BALMO MART
---------------------------

WITH FINAL AS (
    SELECT * FROM {{ ref('staging_v1_ice_balmo') }}
)

SELECT * FROM FINAL
ORDER BY trade_date DESC
