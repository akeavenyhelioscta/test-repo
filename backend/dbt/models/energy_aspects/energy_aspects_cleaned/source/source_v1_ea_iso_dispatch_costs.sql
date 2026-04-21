{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ISO Dispatch Costs (wide format pass-through)
-- Grain: 1 row per month
-- 95 columns: dispatch costs and fuel costs by fuel type, plant type, and hub
---------------------------

WITH SOURCE AS (
    SELECT *
    FROM {{ source('energy_aspects_v1', 'iso_dispatch_costs') }}
)

SELECT * FROM SOURCE
ORDER BY date DESC
