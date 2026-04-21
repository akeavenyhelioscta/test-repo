{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Load Model (monthly)
-- Grain: 1 row per month
-- Extracts 2 PJM columns from Monthly ISO Load Model source
---------------------------

WITH SOURCE AS (
    SELECT *
    FROM {{ ref('source_v1_ea_monthly_iso_load_model') }}
),

---------------------------
-- PJM column extraction
---------------------------

PJM AS (
    SELECT
        date
        ,ea_mod_hist_load_norm_weather_and_fcst_load_norm_weather_pjm_mw AS load_norm_weather_mw
        ,ea_actual_load_fcst_load_norm_weather_pjm_mw AS actual_load_norm_weather_mw
    FROM SOURCE
),

FINAL AS (
    SELECT * FROM PJM
)

SELECT * FROM FINAL
ORDER BY date DESC
