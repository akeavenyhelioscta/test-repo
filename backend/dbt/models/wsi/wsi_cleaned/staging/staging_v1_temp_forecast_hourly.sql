{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- DERIVE DATE & HOUR_ENDING
---------------------------

WITH SOURCE AS (
    SELECT * FROM {{ ref('source_v1_hourly_forecast_temp') }}
),

FINAL AS (
    SELECT
        local_time,
        local_time::DATE AS date,
        EXTRACT(HOUR FROM local_time)::INTEGER AS hour_ending,
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
ORDER BY local_time DESC, region, site_id
