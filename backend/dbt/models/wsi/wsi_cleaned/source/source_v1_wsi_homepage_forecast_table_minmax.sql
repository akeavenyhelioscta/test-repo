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
        forecast_datetime::TIMESTAMP as forecast_execution_datetime
        ,forecast_datetime::DATE as forecast_execution_date

        -- forecast dates
        ,forecast_date::DATE as forecast_date

        -- regions
        ,ref_table.region
        ,f.site_id
        ,ref_table.station_name

        -- min/max temps
        ,min as min_temp
        ,max as max_temp
        ,min_normals as min_temp_normals
        ,max_normals as max_temp_normals

    FROM {{ source('wsi_v1', 'wsi_homepage_forecast_table_minmax_v1_2026_jan_12') }} f
    LEFT JOIN {{ ref('wsi_trader_city_ids_v1_2026_jan_07') }} ref_table ON f.site_id = ref_table.site_id

    WHERE
        forecast_datetime::DATE >= (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE - 10
),

------------------------------------------------------------------
-- Now we get the latest revision for each forecast
------------------------------------------------------------------

TEMP_FORECASTS_REVISION AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY site_id, forecast_date ORDER BY site_id ASC, forecast_execution_datetime ASC, forecast_date ASC) AS forecast_revision
    FROM TEMP_FORECASTS_DAILY
),

TEMP_FORECASTS_REVISION_MAX AS (
    SELECT
        *,
        MAX(forecast_revision) OVER (PARTITION BY site_id, forecast_execution_datetime, forecast_date) AS max_forecast_revision
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
        ,MAX(count_forecast_days) OVER (PARTITION BY forecast_execution_datetime, site_id) AS max_forecast_days
    FROM TEMP_FORECASTS_COUNT
),

-- Note: a forecast should have 14 days
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

        ,f.region
        ,f.site_id
        ,f.station_name

        ,min_temp
        ,max_temp
        ,min_temp_normals
        ,max_temp_normals

    FROM TEMP_FORECASTS_COUNT_FINAL f
    JOIN TEMP_FORECASTS_RANK_LABELLED rank ON f.forecast_execution_datetime = rank.forecast_execution_datetime
)

SELECT * FROM TEMP_FORECASTS_FINAL
ORDER BY forecast_execution_datetime desc, site_id asc, station_name asc, forecast_date asc
