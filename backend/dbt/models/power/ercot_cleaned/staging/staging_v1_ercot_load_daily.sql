{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Load Daily
-- Grain: 1 row per date x period
---------------------------

{% set onpeak_start = 7 %}
{% set onpeak_end = 22 %}

{% set load_zones = ['load_total', 'load_north', 'load_south', 'load_west', 'load_houston'] %}

WITH HOURLY AS (
    SELECT * FROM {{ ref('source_v1_ercot_load_hourly') }}
),

---------------------------
-- FLAT (all hours)
---------------------------

FLAT AS (
    SELECT
        date
        ,'flat' AS period
        {% for zone in load_zones %}
        ,AVG({{ zone }}) AS {{ zone }}
        {% endfor %}
    FROM HOURLY
    GROUP BY date
),

---------------------------
-- ONPEAK (hours 7-22)
---------------------------

ONPEAK AS (
    SELECT
        date
        ,'onpeak' AS period
        {% for zone in load_zones %}
        ,AVG({{ zone }}) AS {{ zone }}
        {% endfor %}
    FROM HOURLY
    WHERE hour_ending BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY date
),

---------------------------
-- OFFPEAK (hours 1-6, 23-24)
---------------------------

OFFPEAK AS (
    SELECT
        date
        ,'offpeak' AS period
        {% for zone in load_zones %}
        ,AVG({{ zone }}) AS {{ zone }}
        {% endfor %}
    FROM HOURLY
    WHERE hour_ending NOT BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY date
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
