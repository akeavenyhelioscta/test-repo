{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- NA Power Price, Heat Rate & Spark Forecasts (wide format pass-through)
-- Grain: 1 row per month
-- 15 columns: on-peak prices, heat rates, spark spreads for 5 hubs
---------------------------

WITH SOURCE AS (
    SELECT *
    FROM {{ source('energy_aspects_v1', 'na_power_price_heat_rate_spark_forecasts') }}
)

SELECT * FROM SOURCE
ORDER BY date DESC
