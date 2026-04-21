{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- US Regional Power Model (wide format pass-through)
-- Grain: 1 row per month
-- 175 columns: generation by fuel, demand, gas demand, net imports across all ISOs
---------------------------

WITH SOURCE AS (
    SELECT *
    FROM {{ source('energy_aspects_v1', 'us_regional_power_model') }}
)

SELECT * FROM SOURCE
ORDER BY date DESC
