{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Pass through forecast revision metadata + derive forecast_date / hour_ending
-- Grain: 1 row per forecast_execution_datetime_utc × local_time × region × site_id × station_name
--
-- TIMEZONE: local_time is per-station LOCAL PREVAILING time (DST-aware) per
-- WSI Trader API docs page 5 (timeutc=false). EPT conversion happens in the
-- mart, where we know the per-station offset CASE.
---------------------------

WITH SOURCE AS (
    SELECT * FROM {{ ref('source_v1_wsi_hourly_forecast_temp') }}
),

FINAL AS (
    SELECT
        forecast_execution_datetime_utc,

        local_time                                  AS forecast_datetime_local,
        local_time::DATE                            AS forecast_date,
        EXTRACT(HOUR FROM local_time)::INTEGER + 1  AS hour_ending,

        region,
        site_id,
        station_name,
        temperature,
        temperature_diff,
        temperature_normal,
        dewpoint,
        cloud_cover_pct,
        feels_like_temperature,
        feels_like_temperature_diff,
        precipitation,
        wind_direction,
        wind_speed,
        ghi_irradiance,
        created_at,
        updated_at
    FROM SOURCE
)

SELECT * FROM FINAL
