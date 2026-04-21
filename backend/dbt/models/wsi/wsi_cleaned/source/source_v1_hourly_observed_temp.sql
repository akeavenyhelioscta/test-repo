{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- SELECT & CAST
---------------------------

WITH RAW AS (
    SELECT * FROM {{ source('wsi_v1', 'hourly_observed_temp_v2_20250722') }}
),

CASTED AS (
    SELECT
        date::DATE AS date,
        hour::INTEGER AS hour_ending,
        region::TEXT AS region,
        site_id::TEXT AS site_id,
        station_name::TEXT AS station_name,
        temp_f::NUMERIC AS temperature,
        dew_point_f::NUMERIC AS dewpoint,
        cloud_cover_pct::NUMERIC AS cloud_cover_pct,
        wind_dir::NUMERIC AS wind_direction,
        wind_speed_mph::NUMERIC AS wind_speed,
        heat_index_f::NUMERIC AS heat_index,
        wind_chill_f::NUMERIC AS wind_chill,
        rh::NUMERIC AS relative_humidity,
        precip_in::NUMERIC AS precipitation
    FROM RAW
),

FINAL AS (
    SELECT * FROM CASTED
)

SELECT * FROM FINAL
