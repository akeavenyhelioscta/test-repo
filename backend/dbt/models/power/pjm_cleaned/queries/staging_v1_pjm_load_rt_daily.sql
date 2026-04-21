{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Daily Load (normalized)
-- Grain: 1 row per date × region × period
---------------------------

WITH HOURLY AS (
    SELECT * FROM {{ ref('staging_v1_pjm_load_rt_hourly') }}
),

FLAT AS (
    SELECT
        date
        ,region
        ,'flat' AS period
        ,AVG(rt_load_mw) AS rt_load
    FROM HOURLY
    GROUP BY date, region
),

PEAK AS (
    SELECT
        date
        ,region
        ,'peak' AS period
        ,MAX(rt_load_mw) AS rt_load
    FROM HOURLY
    GROUP BY date, region
),

ONPEAK AS (
    SELECT
        date
        ,region
        ,'onpeak' AS period
        ,AVG(rt_load_mw) AS rt_load
    FROM HOURLY
    WHERE hour_ending BETWEEN 8 AND 23
    GROUP BY date, region
),

OFFPEAK AS (
    SELECT
        date
        ,region
        ,'offpeak' AS period
        ,AVG(rt_load_mw) AS rt_load
    FROM HOURLY
    WHERE hour_ending NOT BETWEEN 8 AND 23
    GROUP BY date, region
),

DAILY AS (
    SELECT * FROM FLAT
    UNION ALL
    SELECT * FROM PEAK
    UNION ALL
    SELECT * FROM ONPEAK
    UNION ALL
    SELECT * FROM OFFPEAK
)

SELECT * FROM DAILY
ORDER BY date DESC, region, period
