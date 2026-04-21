{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Monthly ISO Load Model (wide format pass-through)
-- Grain: 1 row per month
-- 16 columns: weather-normalized load forecasts and actual load by ISO
---------------------------

WITH SOURCE AS (
    SELECT *
    FROM {{ source('energy_aspects_v1', 'monthly_iso_load_model') }}
)

SELECT * FROM SOURCE
ORDER BY date DESC
