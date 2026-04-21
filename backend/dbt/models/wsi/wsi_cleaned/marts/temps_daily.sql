{{
  config(
    materialized='view'
  )
}}

-------------------------------------------------------------
-- COMBINED DAILY TEMPS: LATEST FORECAST + OBSERVED
-------------------------------------------------------------
--
-- "Latest complete forecast" is defined as:
--   1. Deduplication: latest revision per (site_id, forecast_date) — handled
--      by source model via ROW_NUMBER() keeping max revision.
--   2. Completeness: max_forecast_days >= 14 — enforced by source model.
--   3. Ranking: rank_forecast_execution_timestamps = 1 (most recent complete
--      execution across all stations).
--
-- Grain: date x region x site_id x station_name
--

-------------------------------------------------------------
-- LATEST FORECASTS (rank 1 from city-level weighted source)
-------------------------------------------------------------

WITH LATEST_FORECASTS AS (
    SELECT
        forecast_date,
        region,
        site_id,
        station_name,
        forecast_execution_datetime,
        forecast_execution_date,
        count_forecast_days,
        max_forecast_days,
        min_temp,
        max_temp,
        average_temp,
        cdd,
        hdd
    FROM {{ ref('source_v1_weighted_daily_forecast_temp_city') }}
    WHERE rank_forecast_execution_timestamps = 1
),

-------------------------------------------------------------
-- OBSERVED
-------------------------------------------------------------

OBSERVED AS (
    SELECT
        date,
        region,
        site_id,
        station_name,
        temp,
        temp_min,
        temp_max,
        cdd,
        hdd
    FROM {{ ref('temp_observed_daily') }}
),

-------------------------------------------------------------
-- COMBINE: forecast + observed
-------------------------------------------------------------

FINAL AS (
    SELECT
        f.forecast_date AS date,
        f.region,
        f.site_id,
        f.station_name,
        f.forecast_execution_datetime,
        f.forecast_execution_date,
        f.count_forecast_days,
        f.max_forecast_days,

        -- forecast values
        f.min_temp AS fcst_min_temp,
        f.max_temp AS fcst_max_temp,
        f.average_temp AS fcst_avg_temp,
        f.cdd AS fcst_cdd,
        f.hdd AS fcst_hdd,

        -- observed values (NULL for future dates)
        o.temp AS obs_temp,
        o.temp_min AS obs_temp_min,
        o.temp_max AS obs_temp_max,
        o.cdd AS obs_cdd,
        o.hdd AS obs_hdd

    FROM LATEST_FORECASTS f
    LEFT JOIN OBSERVED o
        ON f.forecast_date = o.date
        AND f.site_id = o.site_id
)

SELECT * FROM FINAL
ORDER BY date, region, site_id
