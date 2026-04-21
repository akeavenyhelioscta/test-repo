{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- SELECT & CAST
---------------------------

WITH RAW AS (
    SELECT * FROM {{ source('wsi_v1', 'hourly_forecast_temp_v4_2025_jan_12') }}
),

CASTED AS (
    SELECT
        local_time::TIMESTAMP AS local_time,
        region::TEXT AS region,
        site_id::TEXT AS site_id,
        station_name::TEXT AS station_name,
        temp::NUMERIC AS temperature,
        tempdiff::NUMERIC AS temperature_diff,
        tempnormal::NUMERIC AS temperature_normal,
        dewpoint::NUMERIC AS dewpoint,
        cloud_cover::NUMERIC AS cloud_cover_pct,
        feelsliketemp::NUMERIC AS feels_like_temperature,
        feelsliketempdiff::NUMERIC AS feels_like_temperature_diff,
        precip::NUMERIC AS precipitation,
        winddir::NUMERIC AS wind_direction,
        windspeed_mph::NUMERIC AS wind_speed,
        ghirradiance::NUMERIC AS ghi_irradiance,
        created_at::TIMESTAMP AS created_at,
        updated_at::TIMESTAMP AS updated_at
    FROM RAW
),

FINAL AS (
    SELECT * FROM CASTED
)

SELECT * FROM FINAL
ORDER BY local_time DESC, region, site_id
