{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- DERIVE observed_datetime + PJM-convention hour_ending (1..24)
-- Grain: 1 row per date × hour × region × site_id × station_name
--
-- TIMEZONE: WSI HISTORICAL_HOURLY_OBSERVED returns LOCAL STANDARD time year-
-- round (NOT DST-aware), per page 20 of the WSI Trader API docs. So Boston
-- Hour=0 = 00:00 EST always = 05:00 UTC always. In summer, that is 01:00 EDT
-- (Eastern *prevailing*) — i.e. the WSI forecast endpoint, which DOES use
-- local prevailing time, will be offset by 1h in absolute time vs this
-- observation for the same station during DST. Joining observed↔forecast
-- on (date, hour_ending) requires a timezone normalization (see the combined
-- view query in the migration notes).
---------------------------

WITH SOURCE AS (
    SELECT * FROM {{ ref('source_v1_wsi_hourly_observed_temp') }}
),

FINAL AS (
    SELECT
        -- naive timestamp in LOCAL STANDARD timezone (e.g. EST for Boston)
        date + (hour_starting || ' hours')::INTERVAL  AS observed_datetime_local_standard,
        date,
        hour_starting,
        hour_starting + 1                              AS hour_ending,  -- PJM convention 1..24
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
ORDER BY observed_datetime_local_standard DESC, region, site_id
