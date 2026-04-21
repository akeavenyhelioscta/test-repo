{{
  config(
    materialized='view'
  )
}}

-------------------------------------------------------------
-- COMBINED DAILY WDD: LATEST FORECASTS + OBSERVED + NORMALS
-------------------------------------------------------------
--
-- "Latest complete forecast" is defined as:
--   1. forecast_rank = 1 in wdd_forecasts_daily, meaning the most recent
--      forecast_execution_datetime per model (NWP) or per WSI blend.
--   2. Only runs whose staging pipeline confirmed exactly 15 forecast days
--      (enforced by staging_v1_wdd_forecast_2_complete).
--   3. bias_corrected = false (raw model output, not post-processed).
--   4. Forecast-difference columns are excluded (no run-over-run deltas).
--
-- Grain: date x region x model x cycle
--

-------------------------------------------------------------
-- LATEST FORECASTS (rank 1, bias_corrected = false)
-------------------------------------------------------------

WITH LATEST_FORECASTS AS (
    SELECT
        forecast_date,
        region,
        model,
        cycle,
        electric_cdd,
        electric_hdd,
        gas_cdd,
        gas_hdd,
        pw_cdd,
        pw_hdd
    FROM {{ ref('wdd_forecasts_daily') }}
    WHERE forecast_rank = 1
        AND bias_corrected = 'false'
),

-------------------------------------------------------------
-- OBSERVED (restrict to 6 forecast-matching regions)
-------------------------------------------------------------

OBSERVED AS (
    SELECT
        date,
        region,
        electric_cdd,
        electric_hdd,
        gas_cdd,
        gas_hdd,
        population_cdd,
        population_hdd
    FROM {{ ref('wdd_observed_daily') }}
    WHERE region IN ('CONUS', 'EAST', 'MIDWEST', 'MOUNTAIN', 'PACIFIC', 'SOUTHCENTRAL')
),

-------------------------------------------------------------
-- 10-YEAR NORMALS
-------------------------------------------------------------

NORMALS AS (
    SELECT
        mm_dd,
        region,
        electric_cdd_normal,
        electric_hdd_normal,
        gas_cdd_normal,
        gas_hdd_normal,
        population_cdd_normal,
        population_hdd_normal,
        tdd_normal
    FROM {{ ref('wdd_normals_daily') }}
    WHERE period = '10_year'
),

-------------------------------------------------------------
-- COMBINE: forecast + observed + normals
-------------------------------------------------------------

FINAL AS (
    SELECT
        f.forecast_date AS date,
        f.region,
        f.model,
        f.cycle,

        -- coalesce: observed when available, forecast otherwise
        COALESCE(o.electric_cdd, f.electric_cdd) AS electric_cdd,
        COALESCE(o.electric_hdd, f.electric_hdd) AS electric_hdd,
        COALESCE(o.gas_cdd, f.gas_cdd) AS gas_cdd,
        COALESCE(o.gas_hdd, f.gas_hdd) AS gas_hdd,
        COALESCE(o.population_cdd, f.pw_cdd) AS pw_cdd,
        COALESCE(o.population_hdd, f.pw_hdd) AS pw_hdd,
        COALESCE(o.gas_hdd + o.population_cdd, f.gas_hdd + f.pw_cdd) AS tdd,

        -- forecast values
        f.electric_cdd AS fcst_electric_cdd,
        f.electric_hdd AS fcst_electric_hdd,
        f.gas_cdd AS fcst_gas_cdd,
        f.gas_hdd AS fcst_gas_hdd,
        f.pw_cdd AS fcst_pw_cdd,
        f.pw_hdd AS fcst_pw_hdd,
        f.gas_hdd + f.pw_cdd AS fcst_tdd,

        -- observed values (NULL for future dates)
        o.electric_cdd AS obs_electric_cdd,
        o.electric_hdd AS obs_electric_hdd,
        o.gas_cdd AS obs_gas_cdd,
        o.gas_hdd AS obs_gas_hdd,
        o.population_cdd AS obs_pw_cdd,
        o.population_hdd AS obs_pw_hdd,

        -- 10-year normals
        n.electric_cdd_normal AS nrm_electric_cdd,
        n.electric_hdd_normal AS nrm_electric_hdd,
        n.gas_cdd_normal AS nrm_gas_cdd,
        n.gas_hdd_normal AS nrm_gas_hdd,
        n.population_cdd_normal AS nrm_pw_cdd,
        n.population_hdd_normal AS nrm_pw_hdd,
        n.tdd_normal AS nrm_tdd

    FROM LATEST_FORECASTS f
    LEFT JOIN OBSERVED o
        ON f.forecast_date = o.date
        AND f.region = o.region
    LEFT JOIN NORMALS n
        ON CASE
            WHEN EXTRACT(MONTH FROM f.forecast_date) = 2
                AND EXTRACT(DAY FROM f.forecast_date) = 29
            THEN '02-28'
            ELSE TO_CHAR(f.forecast_date, 'MM-DD')
        END = n.mm_dd
        AND f.region = n.region
)

SELECT * FROM FINAL
ORDER BY date, region, model, cycle
