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
        (forecast_execution_date)::TIMESTAMP as forecast_execution_datetime
        ,forecast_execution_date::DATE as forecast_execution_date

        -- forecast dates
        ,forecast_date::DATE as forecast_date

        -- model
        ,model
        ,bias_corrected

        -- regions
        ,region

        -- temps and degree days
        ,min_temp
        ,max_temp
        ,min_temp_diff
        ,max_temp_diff
        ,min_temp_dfn
        ,max_temp_dfn
        ,average_temp_dfn
        ,hdd
        ,hdd_diff
        ,cdd
        ,cdd_diff
        ,heat_index
        ,heat_index_diff
        ,diff_from_max_temp

    FROM {{ source('wsi_v1', 'weighted_temp_daily_forecast_iso_wsi_v2_2026_jan_12') }}

    WHERE forecast_execution_date::DATE >= (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE - 10
        AND MODEL IN ('WSI')
),

------------------------------------------------------------------
-- Now we get the latest revision for each forecast
------------------------------------------------------------------

TEMP_FORECASTS_REVISION AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region, forecast_date, model, bias_corrected ORDER BY region ASC, forecast_execution_datetime ASC, forecast_date ASC, model ASC) AS forecast_revision
    FROM TEMP_FORECASTS_DAILY
),

TEMP_FORECASTS_REVISION_MAX AS (
    SELECT
        *,
        MAX(forecast_revision) OVER (PARTITION BY region, forecast_execution_datetime, forecast_date, model, bias_corrected) AS max_forecast_revision
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

        -- get the latest forecast
        ,max(forecast_execution_datetime) OVER () as latest_forecast_execution_datetime

        -- rank forecasts from most recent to latest forecast
        ,DENSE_RANK() OVER (ORDER BY forecast_execution_datetime DESC) as rank_forecast_execution_timestamps

        -- get friday forecast
        ,forecast_execution_datetime::DATE <> (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
            AND EXTRACT(DOW FROM forecast_execution_datetime::DATE) = 5
        as is_friday

        -- rank Friday forecasts to identify the most recent one
        ,DENSE_RANK() OVER (
            PARTITION BY (
                forecast_execution_datetime::DATE <> (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
                AND EXTRACT(DOW FROM forecast_execution_datetime::DATE) = 5
            )
            ORDER BY forecast_execution_datetime DESC
        ) as rank_friday

    FROM (SELECT DISTINCT forecast_execution_datetime FROM TEMP_FORECASTS_COUNT_FINAL) sub
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

        ,f.forecast_date
        ,f.count_forecast_days
        ,f.max_forecast_days

        ,f.model
        ,f.bias_corrected

        ,f.region

        -- temps and degree days
        ,min_temp
        ,max_temp
        ,min_temp_diff
        ,max_temp_diff
        ,min_temp_dfn
        ,max_temp_dfn
        ,average_temp_dfn
        ,hdd
        ,hdd_diff
        ,cdd
        ,cdd_diff
        ,heat_index
        ,heat_index_diff
        ,diff_from_max_temp

    FROM TEMP_FORECASTS_COUNT_FINAL f
    JOIN TEMP_FORECASTS_RANK_LABELLED rank ON f.forecast_execution_datetime = rank.forecast_execution_datetime
)

SELECT * FROM TEMP_FORECASTS_FINAL
ORDER BY region, rank_forecast_execution_timestamps, model, bias_corrected, forecast_date
