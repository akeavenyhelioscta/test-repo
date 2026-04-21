{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- UNPIVOT REGIONS
---------------------------

WITH RAW AS (
    SELECT * FROM {{ source('wsi_v1', 'daily_observed_wdd') }}
),

UNPIVOTED AS (
    SELECT date, data_type, 'EAST' AS region, east AS value FROM RAW
    UNION ALL
    SELECT date, data_type, 'MIDWEST' AS region, midwest AS value FROM RAW
    UNION ALL
    SELECT date, data_type, 'MOUNTAIN' AS region, mountain AS value FROM RAW
    UNION ALL
    SELECT date, data_type, 'PACIFIC' AS region, pacific AS value FROM RAW
    UNION ALL
    SELECT date, data_type, 'SOUTHCENTRAL' AS region, southcentral AS value FROM RAW
    UNION ALL
    SELECT date, data_type, 'CONUS' AS region, conus AS value FROM RAW
    UNION ALL
    SELECT date, data_type, 'GASPRODUCING' AS region, gasproducing AS value FROM RAW
    UNION ALL
    SELECT date, data_type, 'GASCONSEAST' AS region, gasconseast AS value FROM RAW
    UNION ALL
    SELECT date, data_type, 'GASCONSWEST' AS region, gasconswest AS value FROM RAW
),

---------------------------
-- PIVOT DATA TYPES
---------------------------

PIVOTED AS (
    SELECT
        date::DATE AS date,
        region,
        MAX(CASE WHEN UPPER(data_type) = 'ELECTRIC_CDD' THEN value END)::NUMERIC AS electric_cdd,
        MAX(CASE WHEN UPPER(data_type) = 'ELECTRIC_HDD' THEN value END)::NUMERIC AS electric_hdd,
        MAX(CASE WHEN UPPER(data_type) = 'GAS_CDD' THEN value END)::NUMERIC AS gas_cdd,
        MAX(CASE WHEN UPPER(data_type) = 'GAS_HDD' THEN value END)::NUMERIC AS gas_hdd,
        MAX(CASE WHEN UPPER(data_type) = 'OIL_CDD' THEN value END)::NUMERIC AS oil_cdd,
        MAX(CASE WHEN UPPER(data_type) = 'OIL_HDD' THEN value END)::NUMERIC AS oil_hdd,
        MAX(CASE WHEN UPPER(data_type) = 'POPULATION_CDD' THEN value END)::NUMERIC AS population_cdd,
        MAX(CASE WHEN UPPER(data_type) = 'POPULATION_HDD' THEN value END)::NUMERIC AS population_hdd
    FROM UNPIVOTED
    GROUP BY date, region
),

FINAL AS (
    SELECT * FROM PIVOTED
)

SELECT * FROM FINAL
ORDER BY date desc, region
