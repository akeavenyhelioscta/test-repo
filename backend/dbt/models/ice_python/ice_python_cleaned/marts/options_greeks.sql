{{
  config(
    materialized='view'
  )
}}

---------------------------
-- OPTIONS GREEKS MART
---------------------------
-- Full options chain from the latest snapshot per day.
-- Grain: one row per trade_date x symbol.

WITH FINAL AS (
    SELECT * FROM {{ ref('staging_v1_options_greeks') }}
)

SELECT * FROM FINAL
ORDER BY trade_date DESC, underlying, expiry_code, option_type, strike
