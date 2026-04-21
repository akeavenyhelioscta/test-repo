{{
  config(
    materialized='view'
  )
}}

---------------------------
-- PJM vs Meteologica Net Load Forecast Comparison — DA Cutoff
-- Aligns both RTO net-load forecasts on forecast_date x hour_ending and
-- surfaces per-component diffs (meteo minus PJM) for model evaluation.
-- Grain: 1 row per forecast_date × hour_ending.
-- FULL OUTER JOIN so rows present in only one source are not silently dropped.
---------------------------

WITH pjm AS (
    SELECT * FROM {{ ref('pjm_modelling_net_load_forecast_hourly_da_cutoff') }}
),

meteo AS (
    SELECT * FROM {{ ref('pjm_modelling_meteo_net_load_forecast_hourly_da_cutoff') }}
)

SELECT
    COALESCE(pjm.forecast_datetime, meteo.forecast_datetime)  AS forecast_datetime
    ,COALESCE(pjm.forecast_date,    meteo.forecast_date)      AS forecast_date
    ,COALESCE(pjm.hour_ending,      meteo.hour_ending)        AS hour_ending
    ,COALESCE(pjm.region,           meteo.region)             AS region

    -- ── PJM / GridStatus side ─────────────────────────────────────────────
    ,pjm.forecast_load_mw      AS pjm_forecast_load_mw
    ,pjm.solar_forecast        AS pjm_solar_forecast_mw
    ,pjm.wind_forecast         AS pjm_wind_forecast_mw
    ,pjm.net_load_forecast_mw  AS pjm_net_load_forecast_mw

    -- ── Meteologica side ──────────────────────────────────────────────────
    ,meteo.forecast_load_mw     AS meteo_forecast_load_mw
    ,meteo.solar_forecast_mw    AS meteo_solar_forecast_mw
    ,meteo.wind_forecast_mw     AS meteo_wind_forecast_mw
    ,meteo.net_load_forecast_mw AS meteo_net_load_forecast_mw

    -- ── Diffs (meteo minus PJM) ───────────────────────────────────────────
    ,(meteo.forecast_load_mw     - pjm.forecast_load_mw)     AS load_diff_mw
    ,(meteo.solar_forecast_mw    - pjm.solar_forecast)       AS solar_diff_mw
    ,(meteo.wind_forecast_mw     - pjm.wind_forecast)        AS wind_diff_mw
    ,(meteo.net_load_forecast_mw - pjm.net_load_forecast_mw) AS net_load_diff_mw

    -- ── Issuance timestamps (for traceability) ────────────────────────────
    ,pjm.load_forecast_execution_datetime_local   AS pjm_load_forecast_execution_datetime_local
    ,pjm.solar_forecast_execution_datetime_local  AS pjm_solar_forecast_execution_datetime_local
    ,pjm.wind_forecast_execution_datetime_local   AS pjm_wind_forecast_execution_datetime_local
    ,meteo.load_forecast_execution_datetime_local  AS meteo_load_forecast_execution_datetime_local
    ,meteo.solar_forecast_execution_datetime_local AS meteo_solar_forecast_execution_datetime_local
    ,meteo.wind_forecast_execution_datetime_local  AS meteo_wind_forecast_execution_datetime_local

FROM pjm
FULL OUTER JOIN meteo
    ON pjm.forecast_date = meteo.forecast_date
    AND pjm.hour_ending  = meteo.hour_ending

ORDER BY forecast_date, hour_ending
