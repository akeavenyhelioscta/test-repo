{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Solar Forecast Hourly (GridStatus)
-- Grain: 1 row per rank_forecast_execution_timestamps x forecast_date x hour_ending
-- 6 regions x 4 metrics + HSL system wide
---------------------------

WITH raw_data AS (
    SELECT
        publish_time_local::TIMESTAMP AS forecast_execution_datetime
        ,publish_time_local::DATE AS forecast_execution_date

        ,interval_start_local::TIMESTAMP AS interval_start
        ,interval_start_local::DATE AS forecast_date
        ,(EXTRACT(HOUR FROM interval_start_local::TIMESTAMP) + 1)::INT AS hour_ending

        -- COP HSL (Capacity Operating Plan High Sustained Limit)
        ,cop_hsl_system_wide::NUMERIC AS cop_hsl_system_wide
        ,cop_hsl_centerwest::NUMERIC AS cop_hsl_centerwest
        ,cop_hsl_northwest::NUMERIC AS cop_hsl_northwest
        ,cop_hsl_farwest::NUMERIC AS cop_hsl_farwest
        ,cop_hsl_fareast::NUMERIC AS cop_hsl_fareast
        ,cop_hsl_southeast::NUMERIC AS cop_hsl_southeast
        ,cop_hsl_centereast::NUMERIC AS cop_hsl_centereast

        -- STPPF (Short-Term Photovoltaic Power Forecast)
        ,stppf_system_wide::NUMERIC AS stppf_system_wide
        ,stppf_centerwest::NUMERIC AS stppf_centerwest
        ,stppf_northwest::NUMERIC AS stppf_northwest
        ,stppf_farwest::NUMERIC AS stppf_farwest
        ,stppf_fareast::NUMERIC AS stppf_fareast
        ,stppf_southeast::NUMERIC AS stppf_southeast
        ,stppf_centereast::NUMERIC AS stppf_centereast

        -- PVGRPP (PV Generation Resource Production Potential)
        ,pvgrpp_system_wide::NUMERIC AS pvgrpp_system_wide
        ,pvgrpp_centerwest::NUMERIC AS pvgrpp_centerwest
        ,pvgrpp_northwest::NUMERIC AS pvgrpp_northwest
        ,pvgrpp_farwest::NUMERIC AS pvgrpp_farwest
        ,pvgrpp_fareast::NUMERIC AS pvgrpp_fareast
        ,pvgrpp_southeast::NUMERIC AS pvgrpp_southeast
        ,pvgrpp_centereast::NUMERIC AS pvgrpp_centereast

        -- Generation (actual)
        ,gen_system_wide::NUMERIC AS gen_system_wide
        ,gen_centerwest::NUMERIC AS gen_centerwest
        ,gen_northwest::NUMERIC AS gen_northwest
        ,gen_farwest::NUMERIC AS gen_farwest
        ,gen_fareast::NUMERIC AS gen_fareast
        ,gen_southeast::NUMERIC AS gen_southeast
        ,gen_centereast::NUMERIC AS gen_centereast

        -- HSL System Wide
        ,hsl_system_wide::NUMERIC AS hsl_system_wide

    FROM {{ source('gridstatus_v1', 'ercot_solar_actual_and_forecast_by_geo_region_hourly') }}
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
-- Filter to complete forecasts
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
    WHERE forecast_days >= 2
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

        ,cop_hsl_system_wide
        ,cop_hsl_centerwest
        ,cop_hsl_northwest
        ,cop_hsl_farwest
        ,cop_hsl_fareast
        ,cop_hsl_southeast
        ,cop_hsl_centereast

        ,stppf_system_wide
        ,stppf_centerwest
        ,stppf_northwest
        ,stppf_farwest
        ,stppf_fareast
        ,stppf_southeast
        ,stppf_centereast

        ,pvgrpp_system_wide
        ,pvgrpp_centerwest
        ,pvgrpp_northwest
        ,pvgrpp_farwest
        ,pvgrpp_fareast
        ,pvgrpp_southeast
        ,pvgrpp_centereast

        ,gen_system_wide
        ,gen_centerwest
        ,gen_northwest
        ,gen_farwest
        ,gen_fareast
        ,gen_southeast
        ,gen_centereast

        ,hsl_system_wide
    FROM filtered
)

SELECT * FROM ranked
