{{
  config(
    materialized='view'
  )
}}

---------------------------
-- Meteologica PJM Net Load Forecast — DA Cutoff (bias-safe for training)
-- net_load = load - solar - wind (utility-scale; no BTM)
-- Grain: 1 row per forecast_date × hour_ending (RTO only)
-- INNER JOIN: missing solar or wind forecast drops the row rather than imputing zero.
---------------------------

WITH load AS (
    SELECT *
    FROM {{ ref('pjm_modelling_meteo_load_forecast_hourly_da_cutoff') }}
    WHERE region = 'RTO'
),

solar AS (
    SELECT *
    FROM {{ ref('pjm_modelling_meteo_solar_forecast_hourly_da_cutoff') }}
    WHERE region = 'RTO'
),

wind AS (
    SELECT *
    FROM {{ ref('pjm_modelling_meteo_wind_forecast_hourly_da_cutoff') }}
    WHERE region = 'RTO'
)

SELECT
    load.forecast_datetime
    ,load.forecast_date
    ,load.hour_ending
    ,load.region
    ,load.forecast_load_mw
    ,solar.forecast_generation_mw AS solar_forecast_mw
    ,wind.forecast_generation_mw  AS wind_forecast_mw
    ,(load.forecast_load_mw - solar.forecast_generation_mw - wind.forecast_generation_mw) AS net_load_forecast_mw
    ,load.forecast_execution_datetime_local  AS load_forecast_execution_datetime_local
    ,solar.forecast_execution_datetime_local AS solar_forecast_execution_datetime_local
    ,wind.forecast_execution_datetime_local  AS wind_forecast_execution_datetime_local
FROM load
INNER JOIN solar
    ON load.forecast_date = solar.forecast_date
    AND load.hour_ending = solar.hour_ending
INNER JOIN wind
    ON load.forecast_date = wind.forecast_date
    AND load.hour_ending = wind.hour_ending
