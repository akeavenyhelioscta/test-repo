{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- SELECT & CAST raw WSI hourly forecast temperatures (v5, revision-tracking)
-- Source PK: (forecast_execution_datetime_utc, local_time, region, site_id, station_name)
---------------------------

WITH RAW AS (
    SELECT * FROM {{ source('wsi_v1', 'hourly_forecast_temp_v5_2026_apr_23') }}
),

CASTED AS (
    SELECT
        forecast_execution_datetime_utc::TIMESTAMP AS forecast_execution_datetime_utc,
        local_time::TIMESTAMP                      AS local_time,
        region::TEXT                               AS region,
        site_id::TEXT                              AS site_id,
        station_name::TEXT                         AS station_name,
        temp::NUMERIC                              AS temperature,
        tempdiff::NUMERIC                          AS temperature_diff,
        tempnormal::NUMERIC                        AS temperature_normal,
        dewpoint::NUMERIC                          AS dewpoint,
        cloud_cover::NUMERIC                       AS cloud_cover_pct,
        feelsliketemp::NUMERIC                     AS feels_like_temperature,
        feelsliketempdiff::NUMERIC                 AS feels_like_temperature_diff,
        precip::NUMERIC                            AS precipitation,
        winddir::NUMERIC                           AS wind_direction,
        windspeed_mph::NUMERIC                     AS wind_speed,
        ghirradiance::NUMERIC                      AS ghi_irradiance,
        created_at::TIMESTAMP                      AS created_at,
        updated_at::TIMESTAMP                      AS updated_at
    FROM RAW
)

SELECT * FROM CASTED
