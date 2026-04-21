{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Hourly Load (normalized)
-- Combines metered + prelim + instantaneous, preferring metered
-- Grain: 1 row per date × hour × region
---------------------------

WITH METERED AS (
    SELECT
        date
        ,hour_ending
        ,mkt_region
        ,load_area
        ,load_mw
    FROM {{ ref('source_v1_pjm_hrl_load_metered') }}
    WHERE
        date >= '2014-01-01'
        AND load_area IN ('RTO', 'WEST', 'MIDATL', 'SOUTH')
),

PRELIM AS (
    SELECT
        date
        ,hour_ending
        ,mkt_region
        ,load_area
        ,load_mw
    FROM {{ ref('source_v1_pjm_hrl_load_prelim') }}
    WHERE
        date >= ((CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE - 7)
        AND load_area IN ('RTO', 'WEST', 'MIDATL', 'SOUTH')
),

INSTANTANEOUS_5_MIN AS (
    SELECT
        date
        ,hour_ending
        ,mkt_region
        ,load_area
        ,load_mw
    FROM {{ ref('source_v1_pjm_five_min_load') }}
    WHERE
        date >= ((CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE - 3)
        AND load_area IN ('RTO', 'WEST', 'MIDATL', 'SOUTH')
),

INSTANTANEOUS AS (
    SELECT
        date
        ,hour_ending
        ,mkt_region
        ,load_area
        ,AVG(load_mw) AS load_mw
    FROM INSTANTANEOUS_5_MIN
    GROUP BY date, hour_ending, mkt_region, load_area
),

--------------------------------
-- Combine and rank: metered > prelim > instantaneous
--------------------------------

COMBINED AS (
    SELECT *, 'METERED' AS source FROM METERED
    UNION ALL
    SELECT *, 'PRELIM' AS source FROM PRELIM
    UNION ALL
    SELECT *, 'INSTANTANEOUS' AS source FROM INSTANTANEOUS
),

RANKED AS (
    SELECT
        *
        ,ROW_NUMBER() OVER (
            PARTITION BY date, hour_ending, mkt_region, load_area
            ORDER BY
                CASE
                    WHEN source = 'METERED' THEN 0
                    WHEN source = 'PRELIM' THEN 1
                    WHEN source = 'INSTANTANEOUS' THEN 2
                    ELSE 999
                END
        ) AS rank_source
    FROM COMBINED
),

FINAL AS (
    SELECT
        date
        ,hour_ending
        ,load_area AS region
        ,load_mw AS rt_load_mw
    FROM RANKED
    WHERE rank_source = 1
)

SELECT
    date + (hour_ending || ' hours')::interval AS datetime,
    *
FROM FINAL
ORDER BY date DESC, hour_ending DESC, region
