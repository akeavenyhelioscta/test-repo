{{
  config(
    materialized='view'
  )
}}

---------------------------
-- WSI Hourly Forecast Temp — PJM cities, LATEST REVISION ONLY (view)
-- Grain: 1 row per forecast_datetime_ept × site_id
--
-- Wraps wsi_pjm_hourly_forecast_temp and keeps only the most recent
-- revision per (forecast_datetime_ept, site_id). Use this for "what does
-- the model see right now" queries; use the underlying table directly when
-- you want revision history (e.g. forecast evolution matrix).
---------------------------

WITH RANKED AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY forecast_datetime_ept, site_id
            ORDER BY forecast_execution_datetime_utc DESC
        ) AS rn
    FROM {{ ref('wsi_pjm_hourly_forecast_temp') }}
)

SELECT
    forecast_execution_datetime_utc,
    forecast_execution_date_utc,

    forecast_datetime_ept,
    forecast_date_ept,
    hour_ending_ept,

    forecast_datetime_local_prevailing,
    forecast_date,
    hour_ending,

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
FROM RANKED
WHERE rn = 1
ORDER BY forecast_datetime_ept DESC, site_id
