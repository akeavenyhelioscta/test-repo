{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ADD DATETIME
---------------------------

WITH SOURCE AS (
    SELECT * FROM {{ ref('source_v1_hourly_observed_temp') }}
),

FINAL AS (
    SELECT
        date + (hour_ending || ' hours')::INTERVAL AS datetime,
        date,
        hour_ending,
        region,
        site_id,
        station_name,
        temperature,
        dewpoint,
        cloud_cover_pct,
        wind_direction,
        wind_speed,
        heat_index,
        wind_chill,
        relative_humidity,
        precipitation
    FROM SOURCE
)

SELECT * FROM FINAL
