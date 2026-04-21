{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Lower 48 Average Power Demand (wide format pass-through)
-- Grain: 1 row per month
-- 7 columns: ISO-level demand (caiso, ercot, isone, miso, nyiso, pjm, spp)
---------------------------

WITH SOURCE AS (
    SELECT *
    FROM {{ source('energy_aspects_v1', 'lower_48_average_power_demand_mw') }}
)

SELECT * FROM SOURCE
ORDER BY date DESC
