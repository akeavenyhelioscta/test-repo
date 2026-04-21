{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Wind Forecast Hourly (GridStatus)
-- Grain: 1 row per rank_forecast_execution_timestamps x forecast_date x hour_ending
-- 5 regions x 4 metrics + HSL system wide
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
        ,cop_hsl_panhandle::NUMERIC AS cop_hsl_panhandle
        ,cop_hsl_coastal::NUMERIC AS cop_hsl_coastal
        ,cop_hsl_south::NUMERIC AS cop_hsl_south
        ,cop_hsl_west::NUMERIC AS cop_hsl_west
        ,cop_hsl_north::NUMERIC AS cop_hsl_north

        -- STWPF (Short-Term Wind Power Forecast)
        ,stwpf_system_wide::NUMERIC AS stwpf_system_wide
        ,stwpf_panhandle::NUMERIC AS stwpf_panhandle
        ,stwpf_coastal::NUMERIC AS stwpf_coastal
        ,stwpf_south::NUMERIC AS stwpf_south
        ,stwpf_west::NUMERIC AS stwpf_west
        ,stwpf_north::NUMERIC AS stwpf_north

        -- WGRPP (Wind Generation Resource Production Potential)
        ,wgrpp_system_wide::NUMERIC AS wgrpp_system_wide
        ,wgrpp_panhandle::NUMERIC AS wgrpp_panhandle
        ,wgrpp_coastal::NUMERIC AS wgrpp_coastal
        ,wgrpp_south::NUMERIC AS wgrpp_south
        ,wgrpp_west::NUMERIC AS wgrpp_west
        ,wgrpp_north::NUMERIC AS wgrpp_north

        -- Generation (actual)
        ,gen_system_wide::NUMERIC AS gen_system_wide
        ,gen_panhandle::NUMERIC AS gen_panhandle
        ,gen_coastal::NUMERIC AS gen_coastal
        ,gen_south::NUMERIC AS gen_south
        ,gen_west::NUMERIC AS gen_west
        ,gen_north::NUMERIC AS gen_north

        -- HSL System Wide
        ,hsl_system_wide::NUMERIC AS hsl_system_wide

    FROM {{ source('gridstatus_v1', 'ercot_wind_actual_and_forecast_by_geo_region_hourly') }}
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
        ,cop_hsl_panhandle
        ,cop_hsl_coastal
        ,cop_hsl_south
        ,cop_hsl_west
        ,cop_hsl_north

        ,stwpf_system_wide
        ,stwpf_panhandle
        ,stwpf_coastal
        ,stwpf_south
        ,stwpf_west
        ,stwpf_north

        ,wgrpp_system_wide
        ,wgrpp_panhandle
        ,wgrpp_coastal
        ,wgrpp_south
        ,wgrpp_west
        ,wgrpp_north

        ,gen_system_wide
        ,gen_panhandle
        ,gen_coastal
        ,gen_south
        ,gen_west
        ,gen_north

        ,hsl_system_wide
    FROM filtered
)

SELECT * FROM ranked
