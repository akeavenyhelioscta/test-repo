{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica: usa_pjm_western_hub_da_power_price_forecast_hourly
-- Thin passthrough of raw Meteologica source table (schema: meteologica).
-- Western Hub DA power price — deterministic point forecast.
-- Grain: 1 row per update_id x forecast_period_start
---------------------------

SELECT
    content_id
    ,update_id
    ,issue_date
    ,forecast_period_start
    ,forecast_period_end
    ,day_ahead_price
FROM {{ source('meteologica_pjm_v1', 'usa_pjm_western_hub_da_power_price_forecast_hourly') }}
