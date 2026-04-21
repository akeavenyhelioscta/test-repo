{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Price, Heat Rate & Spark Spread (monthly)
-- Grain: 1 row per month
-- Extracts 3 PJM West columns from NA Power Price/HR/Spark source
---------------------------

WITH SOURCE AS (
    SELECT *
    FROM {{ ref('source_v1_ea_na_power_price_hr_spark') }}
),

---------------------------
-- PJM West column extraction
---------------------------

PJM AS (
    SELECT
        date
        ,fcst_on_peak_power_prices_in_pjm_west_in_usd_mwh AS on_peak_power_price_usd_per_mwh
        ,fcst_on_peak_heat_rate_in_pjm_west_in_mmbtu_per_mwh AS on_peak_heat_rate_mmbtu_per_mwh
        ,fcst_on_peak_dirty_spark_spreads_in_pjm_west_in_usd_mwh AS on_peak_dirty_spark_spread_usd_per_mwh
    FROM SOURCE
),

FINAL AS (
    SELECT * FROM PJM
)

SELECT * FROM FINAL
ORDER BY date DESC
