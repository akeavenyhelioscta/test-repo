{{
  config(
    materialized='view'
  )
}}

---------------------------
-- OPTIONS GREEKS DAILY MART
---------------------------
-- Aggregated daily options summary per underlying and expiration.
-- Grain: one row per trade_date x underlying x expiry_code.

WITH FINAL AS (
    SELECT * FROM {{ ref('staging_v1_options_greeks_daily') }}
)

SELECT * FROM FINAL
ORDER BY trade_date DESC, underlying, expiry_code
