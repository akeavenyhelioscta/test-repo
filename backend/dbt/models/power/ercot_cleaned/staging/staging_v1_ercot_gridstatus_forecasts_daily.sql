{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Forecasts Daily
-- Grain: 1 row per rank_forecast_execution_timestamps x forecast_date x period
---------------------------

{% set onpeak_start = 7 %}
{% set onpeak_end = 22 %}

{% set forecast_cols = [
    'forecast_load_total',
    'forecast_net_load_total',
    'forecast_solar_total',
    'forecast_wind_total'
] %}

WITH HOURLY AS (
    SELECT * FROM {{ ref('staging_v1_ercot_gridstatus_forecasts_hourly') }}
),

---------------------------
-- FLAT (all hours)
---------------------------

FLAT AS (
    SELECT
        rank_forecast_execution_timestamps
        ,labelled_forecast_execution_timestamp
        ,forecast_execution_datetime
        ,forecast_execution_date
        ,forecast_date
        ,'flat' AS period
        {% for col in forecast_cols %}
        ,AVG({{ col }}) AS {{ col }}
        {% endfor %}
    FROM HOURLY
    GROUP BY rank_forecast_execution_timestamps, labelled_forecast_execution_timestamp,
             forecast_execution_datetime, forecast_execution_date, forecast_date
),

---------------------------
-- ONPEAK (hours 7-22)
---------------------------

ONPEAK AS (
    SELECT
        rank_forecast_execution_timestamps
        ,labelled_forecast_execution_timestamp
        ,forecast_execution_datetime
        ,forecast_execution_date
        ,forecast_date
        ,'onpeak' AS period
        {% for col in forecast_cols %}
        ,AVG({{ col }}) AS {{ col }}
        {% endfor %}
    FROM HOURLY
    WHERE hour_ending BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY rank_forecast_execution_timestamps, labelled_forecast_execution_timestamp,
             forecast_execution_datetime, forecast_execution_date, forecast_date
),

---------------------------
-- OFFPEAK (hours 1-6, 23-24)
---------------------------

OFFPEAK AS (
    SELECT
        rank_forecast_execution_timestamps
        ,labelled_forecast_execution_timestamp
        ,forecast_execution_datetime
        ,forecast_execution_date
        ,forecast_date
        ,'offpeak' AS period
        {% for col in forecast_cols %}
        ,AVG({{ col }}) AS {{ col }}
        {% endfor %}
    FROM HOURLY
    WHERE hour_ending NOT BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY rank_forecast_execution_timestamps, labelled_forecast_execution_timestamp,
             forecast_execution_datetime, forecast_execution_date, forecast_date
),

---------------------------
-- UNION ALL PERIODS
---------------------------

DAILY AS (
    SELECT * FROM FLAT
    UNION ALL
    SELECT * FROM ONPEAK
    UNION ALL
    SELECT * FROM OFFPEAK
)

SELECT * FROM DAILY
