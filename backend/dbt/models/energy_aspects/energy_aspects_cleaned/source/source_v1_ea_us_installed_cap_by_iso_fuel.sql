{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- US Installed Capacity by ISO and Fuel Type (wide format pass-through)
-- Grain: 1 row per month
-- 84 columns: installed capacity by fuel type across all ISOs
---------------------------

WITH SOURCE AS (
    SELECT *
    FROM {{ source('energy_aspects_v1', 'us_installed_capacity_by_iso_and_fuel_type') }}
)

SELECT * FROM SOURCE
ORDER BY date DESC
