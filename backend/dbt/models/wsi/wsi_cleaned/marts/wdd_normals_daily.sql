{{
  config(
    materialized='table'
  )
}}

-------------------------------------------------------------
-- BASE: single scan of source, fold Feb 29 into Feb 28
-------------------------------------------------------------

WITH base AS (
    SELECT
        EXTRACT(YEAR FROM date) AS yr,
        CASE
            WHEN EXTRACT(MONTH FROM date) = 2 AND EXTRACT(DAY FROM date) = 29
            THEN '02-28'
            ELSE TO_CHAR(date, 'MM-DD')
        END AS mm_dd,
        region,
        electric_cdd,
        electric_hdd,
        gas_cdd,
        gas_hdd,
        population_cdd,
        population_hdd,
        gas_hdd + population_cdd AS tdd
    FROM {{ ref('source_v1_daily_observed_wdd') }}
    WHERE
        EXTRACT(YEAR FROM date) BETWEEN
            EXTRACT(YEAR FROM CURRENT_DATE) - 30
            AND EXTRACT(YEAR FROM CURRENT_DATE) - 1
),

-------------------------------------------------------------
-- 10-YEAR NORMALS
-------------------------------------------------------------

normals_10_year AS (
    SELECT
        mm_dd,
        EXTRACT(MONTH FROM TO_DATE(mm_dd, 'MM-DD')) AS month,
        region,
        '10_year' AS period,

        MIN(yr) AS min_year,
        MAX(yr) AS max_year,
        COUNT(DISTINCT yr) AS years_count,

        AVG(electric_cdd) AS electric_cdd_normal,
        AVG(electric_hdd) AS electric_hdd_normal,
        AVG(gas_cdd) AS gas_cdd_normal,
        AVG(gas_hdd) AS gas_hdd_normal,
        AVG(population_cdd) AS population_cdd_normal,
        AVG(population_hdd) AS population_hdd_normal,
        AVG(tdd) AS tdd_normal,

        MIN(electric_cdd) AS electric_cdd_min,
        MIN(electric_hdd) AS electric_hdd_min,
        MIN(gas_cdd) AS gas_cdd_min,
        MIN(gas_hdd) AS gas_hdd_min,
        MIN(population_cdd) AS population_cdd_min,
        MIN(population_hdd) AS population_hdd_min,
        MIN(tdd) AS tdd_min,

        MAX(electric_cdd) AS electric_cdd_max,
        MAX(electric_hdd) AS electric_hdd_max,
        MAX(gas_cdd) AS gas_cdd_max,
        MAX(gas_hdd) AS gas_hdd_max,
        MAX(population_cdd) AS population_cdd_max,
        MAX(population_hdd) AS population_hdd_max,
        MAX(tdd) AS tdd_max,

        STDDEV(electric_cdd) AS electric_cdd_stddev,
        STDDEV(electric_hdd) AS electric_hdd_stddev,
        STDDEV(gas_cdd) AS gas_cdd_stddev,
        STDDEV(gas_hdd) AS gas_hdd_stddev,
        STDDEV(population_cdd) AS population_cdd_stddev,
        STDDEV(population_hdd) AS population_hdd_stddev,
        STDDEV(tdd) AS tdd_stddev

    FROM base
    WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 10
    GROUP BY mm_dd, region
),

-------------------------------------------------------------
-- 30-YEAR NORMALS
-------------------------------------------------------------

normals_30_year AS (
    SELECT
        mm_dd,
        EXTRACT(MONTH FROM TO_DATE(mm_dd, 'MM-DD')) AS month,
        region,
        '30_year' AS period,

        MIN(yr) AS min_year,
        MAX(yr) AS max_year,
        COUNT(DISTINCT yr) AS years_count,

        AVG(electric_cdd) AS electric_cdd_normal,
        AVG(electric_hdd) AS electric_hdd_normal,
        AVG(gas_cdd) AS gas_cdd_normal,
        AVG(gas_hdd) AS gas_hdd_normal,
        AVG(population_cdd) AS population_cdd_normal,
        AVG(population_hdd) AS population_hdd_normal,
        AVG(tdd) AS tdd_normal,

        MIN(electric_cdd) AS electric_cdd_min,
        MIN(electric_hdd) AS electric_hdd_min,
        MIN(gas_cdd) AS gas_cdd_min,
        MIN(gas_hdd) AS gas_hdd_min,
        MIN(population_cdd) AS population_cdd_min,
        MIN(population_hdd) AS population_hdd_min,
        MIN(tdd) AS tdd_min,

        MAX(electric_cdd) AS electric_cdd_max,
        MAX(electric_hdd) AS electric_hdd_max,
        MAX(gas_cdd) AS gas_cdd_max,
        MAX(gas_hdd) AS gas_hdd_max,
        MAX(population_cdd) AS population_cdd_max,
        MAX(population_hdd) AS population_hdd_max,
        MAX(tdd) AS tdd_max,

        STDDEV(electric_cdd) AS electric_cdd_stddev,
        STDDEV(electric_hdd) AS electric_hdd_stddev,
        STDDEV(gas_cdd) AS gas_cdd_stddev,
        STDDEV(gas_hdd) AS gas_hdd_stddev,
        STDDEV(population_cdd) AS population_cdd_stddev,
        STDDEV(population_hdd) AS population_hdd_stddev,
        STDDEV(tdd) AS tdd_stddev

    FROM base
    GROUP BY mm_dd, region
)

SELECT * FROM normals_10_year
UNION ALL
SELECT * FROM normals_30_year
ORDER BY mm_dd, region, period
