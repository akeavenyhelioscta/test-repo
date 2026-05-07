{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM Western-Hub DA Price Forecast — deterministic point (Hourly)
-- Single source table (Western-Hub only — single price node, no region dim).
-- Produces UTC/timezone/local triplets for issue time, ranks by issue time
-- (most recent first).
-- Grain: 1 row per forecast_execution_datetime x forecast_date x hour_ending
---------------------------

WITH RAW AS (
    SELECT
        update_id
        ,issue_date
        ,forecast_period_start
        ,day_ahead_price
    FROM {{ ref('src_meteo_pjm_wh_da_price') }}
),

---------------------------
-- NORMALIZE TIMESTAMPS (UTC + timezone + local triplets)
-- issue_date is published in UTC (varchar in raw); cast to TIMESTAMP and
-- convert to America/New_York for EPT local.
-- forecast_period_start is an hour-beginning local timestamp (no tz).
-- hour_ending = EXTRACT(HOUR FROM forecast_period_start) + 1 (1..24).
---------------------------

NORMALIZED AS (
    SELECT
        issue_date::TIMESTAMP AS forecast_execution_datetime_utc
        ,'US/Eastern' AS timezone
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS forecast_execution_datetime_local
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE AS forecast_execution_date

        ,forecast_period_start::DATE AS forecast_date
        ,EXTRACT(HOUR FROM forecast_period_start)::INT + 1 AS hour_ending

        ,day_ahead_price::NUMERIC AS da_price_deterministic
    FROM RAW
),

--------------------------------
-- Rank forecasts per forecast_date by issue time (most recent first).
--------------------------------

FORECAST_RANK AS (
    SELECT
        forecast_date
        ,forecast_execution_datetime_local

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date
            ORDER BY forecast_execution_datetime_local DESC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_datetime_local, forecast_date
        FROM NORMALIZED
    ) sub
),

--------------------------------
-- FINAL
--------------------------------

FINAL AS (
    SELECT
        n.forecast_execution_datetime_utc
        ,n.timezone
        ,n.forecast_execution_datetime_local
        ,r.forecast_rank
        ,n.forecast_execution_date

        ,(n.forecast_date + INTERVAL '1 hour' * (n.hour_ending - 1)) AS forecast_datetime
        ,n.forecast_date
        ,n.hour_ending

        ,n.da_price_deterministic

    FROM NORMALIZED n
    JOIN FORECAST_RANK r
        ON n.forecast_execution_datetime_local = r.forecast_execution_datetime_local
        AND n.forecast_date = r.forecast_date
)

SELECT * FROM FINAL
ORDER BY forecast_date DESC, forecast_execution_datetime_local DESC, hour_ending
