{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH TEMP_FORECASTS_DAILY AS (
    select

        -- execution of the forecast
        (forecast_execution_date || ' ' || REPLACE(cycle, 'Z', ':00:00'))::TIMESTAMP as forecast_execution_datetime
        ,forecast_execution_date::DATE

        -- forecast dates
        ,forecast_date::DATE as forecast_date

        -- model
        ,model
        ,cycle
        ,bias_corrected

        -- regions
        ,region

        -- temps and degree days
        ,max_temp
        ,min_temp
        ,average_temp_dfn
        ,cdd
        ,hdd

    FROM {{ source('wsi_v1', 'weighted_temp_daily_forecast_iso_models_v2_2026_jan_12') }}

    WHERE forecast_execution_date::DATE >= (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE - 10
        AND MODEL IN ('GFS_OP', 'GFS_ENS', 'ECMWF_OP', 'ECMWF_ENS')
        AND cycle IN ('00Z', '12Z')
),

------------------------------------------------------------------
-- Now we get the latest revision for each forecast
------------------------------------------------------------------

TEMP_FORECASTS_REVISION AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region, forecast_date, cycle, model, bias_corrected ORDER BY region ASC, forecast_execution_datetime ASC, forecast_date ASC, cycle ASC, model ASC) AS forecast_revision
    FROM TEMP_FORECASTS_DAILY
),

TEMP_FORECASTS_REVISION_MAX AS (
    SELECT
        *,
        MAX(forecast_revision) OVER (PARTITION BY region, forecast_execution_datetime, forecast_date, cycle, model, bias_corrected) AS max_forecast_revision
    FROM TEMP_FORECASTS_REVISION
),

TEMP_FORECASTS_REVISION_FINAL AS (
    SELECT
        *
    FROM TEMP_FORECASTS_REVISION_MAX
    WHERE forecast_revision = max_forecast_revision
),

------------------------------------------------------------------
-- Now we check for complete forecasts
------------------------------------------------------------------

TEMP_FORECASTS_COUNT AS (
    SELECT
        *
        ,(forecast_date - forecast_execution_date) + 1 AS count_forecast_days
    FROM TEMP_FORECASTS_REVISION_FINAL
),

TEMP_FORECASTS_COUNT_MAX AS (
    SELECT
        *
        ,MAX(count_forecast_days) OVER (PARTITION BY forecast_execution_datetime, model, region, bias_corrected) AS max_forecast_days
    FROM TEMP_FORECASTS_COUNT
),

-- Note: a forecast should have 15 days
TEMP_FORECASTS_COUNT_FINAL AS (
    SELECT * FROM TEMP_FORECASTS_COUNT_MAX
    WHERE max_forecast_days >= 14
),

------------------------------------------------------------------
-- Now we create a rank for forecasts
------------------------------------------------------------------

TEMP_FORECASTS_RANK AS (
    SELECT

        forecast_execution_datetime
        ,cycle

        -- get the latest forecast
        ,MAX(forecast_execution_datetime) OVER () as latest_forecast_execution_datetime

        -- rank forecasts from most recent to latest forecast
        ,DENSE_RANK() OVER (ORDER BY forecast_execution_datetime DESC) as rank_forecast_execution_timestamps

        -- get fri 12z forecast
        ,forecast_execution_datetime::DATE <> (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
            AND EXTRACT(DOW FROM forecast_execution_datetime::DATE) = 5
            AND EXTRACT(HOUR FROM forecast_execution_datetime::TIMESTAMP) = 12
            AND EXTRACT(MINUTE FROM forecast_execution_datetime::TIMESTAMP) = 0
        as is_friday_12z

        -- rank Friday 12z forecasts to identify the most recent one
        ,DENSE_RANK() OVER (
            PARTITION BY (
                forecast_execution_datetime::DATE <> (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
                AND EXTRACT(DOW FROM forecast_execution_datetime::DATE) = 5
                AND EXTRACT(HOUR FROM forecast_execution_datetime::TIMESTAMP) = 12
                AND EXTRACT(MINUTE FROM forecast_execution_datetime::TIMESTAMP) = 0
            )
            ORDER BY forecast_execution_datetime DESC
        ) as rank_friday_12z

    FROM (SELECT DISTINCT forecast_execution_datetime, cycle FROM TEMP_FORECASTS_COUNT_FINAL) sub
),

TEMP_FORECASTS_RANK_LABELLED AS (
    SELECT

        *

        -- label forecasts
        ,CASE
            WHEN rank_forecast_execution_timestamps = 1 THEN 'Current Forecast'
            ELSE NULL
        END AS labelled_forecast_execution_timestamp

    FROM TEMP_FORECASTS_RANK
),

------------------------------------------------------------------
------------------------------------------------------------------

TEMP_FORECASTS_FINAL AS (
    SELECT

        rank.rank_forecast_execution_timestamps
        ,rank.labelled_forecast_execution_timestamp

        ,f.forecast_execution_datetime
        ,f.forecast_execution_date
        ,f.cycle

        ,f.forecast_date
        ,f.count_forecast_days
        ,f.max_forecast_days

        ,f.model
        ,f.bias_corrected

        ,f.region

        ,f.min_temp
        ,f.max_temp
        ,f.average_temp_dfn
        ,f.cdd
        ,f.hdd

    FROM TEMP_FORECASTS_COUNT_FINAL f
    JOIN TEMP_FORECASTS_RANK_LABELLED rank ON f.forecast_execution_datetime = rank.forecast_execution_datetime
)

SELECT * FROM TEMP_FORECASTS_FINAL
ORDER BY region, rank_forecast_execution_timestamps, model, cycle, bias_corrected, forecast_date
