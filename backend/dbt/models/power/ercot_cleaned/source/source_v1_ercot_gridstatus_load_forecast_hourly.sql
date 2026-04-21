{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Load Forecast Hourly (GridStatus)
-- Grain: 1 row per rank_forecast_execution_timestamps x forecast_date x hour_ending
-- Complex forecast ranking: latest revision per execution, complete forecasts only
---------------------------

WITH raw_data AS (
    SELECT
        publish_time_local::TIMESTAMP AS forecast_execution_datetime
        ,publish_time_local::DATE AS forecast_execution_date

        ,interval_start_local::TIMESTAMP AS interval_start
        ,interval_start_local::DATE AS forecast_date
        ,(EXTRACT(HOUR FROM interval_start_local::TIMESTAMP) + 1)::INT AS hour_ending

        ,north::NUMERIC AS load_north
        ,south::NUMERIC AS load_south
        ,west::NUMERIC AS load_west
        ,houston::NUMERIC AS load_houston
        ,(COALESCE(north::NUMERIC, 0)
            + COALESCE(south::NUMERIC, 0)
            + COALESCE(west::NUMERIC, 0)
            + COALESCE(houston::NUMERIC, 0)
        ) AS load_total

    FROM {{ source('gridstatus_v1', 'ercot_load_forecast_by_forecast_zone') }}
),

--------------------------------
-- Take latest revision per (forecast_execution_date, forecast_date, hour_ending)
--------------------------------

latest_revision AS (
    SELECT
        *
        ,ROW_NUMBER() OVER (
            PARTITION BY forecast_execution_date, forecast_date, hour_ending
            ORDER BY forecast_execution_datetime DESC
        ) AS rn
    FROM raw_data
),

deduped AS (
    SELECT * FROM latest_revision WHERE rn = 1
),

--------------------------------
-- Filter to complete forecasts (24 hours per day, 7+ forecast days)
--------------------------------

forecast_day_counts AS (
    SELECT
        forecast_execution_date
        ,forecast_date
        ,COUNT(*) AS hours_in_day
    FROM deduped
    GROUP BY forecast_execution_date, forecast_date
),

complete_days AS (
    SELECT
        forecast_execution_date
        ,forecast_date
    FROM forecast_day_counts
    WHERE hours_in_day = 24
),

forecast_span AS (
    SELECT
        forecast_execution_date
        ,COUNT(DISTINCT forecast_date) AS forecast_days
    FROM complete_days
    GROUP BY forecast_execution_date
),

complete_forecasts AS (
    SELECT forecast_execution_date
    FROM forecast_span
    WHERE forecast_days >= 7
),

filtered AS (
    SELECT d.*
    FROM deduped d
    INNER JOIN complete_days cd
        ON d.forecast_execution_date = cd.forecast_execution_date
        AND d.forecast_date = cd.forecast_date
    INNER JOIN complete_forecasts cf
        ON d.forecast_execution_date = cf.forecast_execution_date
),

--------------------------------
-- Rank forecasts by recency and label
--------------------------------

ranked AS (
    SELECT
        DENSE_RANK() OVER (ORDER BY forecast_execution_date DESC) AS rank_forecast_execution_timestamps
        ,CASE
            WHEN DENSE_RANK() OVER (ORDER BY forecast_execution_date DESC) = 1 THEN 'Current Forecast'
            WHEN DENSE_RANK() OVER (ORDER BY forecast_execution_date DESC) = 2 THEN 'Previous Forecast'
            ELSE 'Forecast ' || DENSE_RANK() OVER (ORDER BY forecast_execution_date DESC)
        END AS labelled_forecast_execution_timestamp

        ,forecast_execution_datetime
        ,forecast_execution_date

        ,interval_start
        ,forecast_date
        ,hour_ending

        ,load_north
        ,load_south
        ,load_west
        ,load_houston
        ,load_total
    FROM filtered
)

SELECT * FROM ranked
