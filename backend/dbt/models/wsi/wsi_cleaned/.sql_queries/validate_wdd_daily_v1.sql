/*
    VALIDATION v1: wdd_daily combined mart
    Compare output against WSI Forecast Views reference (Gas Weighted HDD, CONUS).

    Reads from the combined mart view wsi_cleaned.wdd_daily which joins:
      - Latest forecast (rank 1, bias_corrected = 'false') from wdd_forecasts_daily
      - Observed from wdd_observed_daily
      - 10-year normals from wdd_normals_daily

    Pivots 15-day window into day columns for forecast, observed, and normals.

    Sections:
      1. Forecast gas_hdd pivoted by model/cycle (matches reference "Model Forecast" rows)
      2. Observed gas_hdd for overlapping past dates
      3. Normals gas_hdd for the same 15-day window
      4. Forecast vs Observed difference for past dates
*/

------------------------------------------------------------
-- SECTION 1: Forecast gas_hdd pivoted (15 days from today)
------------------------------------------------------------

WITH wdd AS (
    SELECT *
    FROM wsi_cleaned.wdd_daily
    WHERE region = 'CONUS'
      AND date >= CURRENT_DATE
      AND date < CURRENT_DATE + 15
),

forecast_pivot AS (
    SELECT
        model,
        COALESCE(cycle, '-') AS cycle,
        MAX(forecast_execution_datetime) AS forecast_execution_datetime,
        'Gas HDD' AS type,
        'Forecast' AS row_type,
        MAX(CASE WHEN date = CURRENT_DATE + 0  THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_01,
        MAX(CASE WHEN date = CURRENT_DATE + 1  THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_02,
        MAX(CASE WHEN date = CURRENT_DATE + 2  THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_03,
        MAX(CASE WHEN date = CURRENT_DATE + 3  THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_04,
        MAX(CASE WHEN date = CURRENT_DATE + 4  THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_05,
        MAX(CASE WHEN date = CURRENT_DATE + 5  THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_06,
        MAX(CASE WHEN date = CURRENT_DATE + 6  THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_07,
        MAX(CASE WHEN date = CURRENT_DATE + 7  THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_08,
        MAX(CASE WHEN date = CURRENT_DATE + 8  THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_09,
        MAX(CASE WHEN date = CURRENT_DATE + 9  THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_10,
        MAX(CASE WHEN date = CURRENT_DATE + 10 THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_11,
        MAX(CASE WHEN date = CURRENT_DATE + 11 THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_12,
        MAX(CASE WHEN date = CURRENT_DATE + 12 THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_13,
        MAX(CASE WHEN date = CURRENT_DATE + 13 THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_14,
        MAX(CASE WHEN date = CURRENT_DATE + 14 THEN ROUND(fcst_gas_hdd::NUMERIC, 1) END) AS day_15
    FROM wdd
    GROUP BY model, COALESCE(cycle, '-')
),

------------------------------------------------------------
-- SECTION 2: Observed gas_hdd (only past dates will be non-NULL)
------------------------------------------------------------

observed_pivot AS (
    SELECT
        'ALL' AS model,
        '-' AS cycle,
        NULL::TIMESTAMP AS forecast_execution_datetime,
        'Gas HDD' AS type,
        'Observed' AS row_type,
        MAX(CASE WHEN date = CURRENT_DATE + 0  THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_01,
        MAX(CASE WHEN date = CURRENT_DATE + 1  THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_02,
        MAX(CASE WHEN date = CURRENT_DATE + 2  THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_03,
        MAX(CASE WHEN date = CURRENT_DATE + 3  THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_04,
        MAX(CASE WHEN date = CURRENT_DATE + 4  THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_05,
        MAX(CASE WHEN date = CURRENT_DATE + 5  THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_06,
        MAX(CASE WHEN date = CURRENT_DATE + 6  THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_07,
        MAX(CASE WHEN date = CURRENT_DATE + 7  THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_08,
        MAX(CASE WHEN date = CURRENT_DATE + 8  THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_09,
        MAX(CASE WHEN date = CURRENT_DATE + 9  THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_10,
        MAX(CASE WHEN date = CURRENT_DATE + 10 THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_11,
        MAX(CASE WHEN date = CURRENT_DATE + 11 THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_12,
        MAX(CASE WHEN date = CURRENT_DATE + 12 THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_13,
        MAX(CASE WHEN date = CURRENT_DATE + 13 THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_14,
        MAX(CASE WHEN date = CURRENT_DATE + 14 THEN ROUND(obs_gas_hdd::NUMERIC, 1) END) AS day_15
    FROM wdd
    WHERE model = 'WSI'  -- observed is the same across all models; pick one to avoid duplication
),

------------------------------------------------------------
-- SECTION 3: 10-year normals gas_hdd
------------------------------------------------------------

normals_pivot AS (
    SELECT
        'ALL' AS model,
        '-' AS cycle,
        NULL::TIMESTAMP AS forecast_execution_datetime,
        'Gas HDD' AS type,
        'Normal' AS row_type,
        MAX(CASE WHEN date = CURRENT_DATE + 0  THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_01,
        MAX(CASE WHEN date = CURRENT_DATE + 1  THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_02,
        MAX(CASE WHEN date = CURRENT_DATE + 2  THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_03,
        MAX(CASE WHEN date = CURRENT_DATE + 3  THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_04,
        MAX(CASE WHEN date = CURRENT_DATE + 4  THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_05,
        MAX(CASE WHEN date = CURRENT_DATE + 5  THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_06,
        MAX(CASE WHEN date = CURRENT_DATE + 6  THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_07,
        MAX(CASE WHEN date = CURRENT_DATE + 7  THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_08,
        MAX(CASE WHEN date = CURRENT_DATE + 8  THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_09,
        MAX(CASE WHEN date = CURRENT_DATE + 9  THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_10,
        MAX(CASE WHEN date = CURRENT_DATE + 10 THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_11,
        MAX(CASE WHEN date = CURRENT_DATE + 11 THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_12,
        MAX(CASE WHEN date = CURRENT_DATE + 12 THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_13,
        MAX(CASE WHEN date = CURRENT_DATE + 13 THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_14,
        MAX(CASE WHEN date = CURRENT_DATE + 14 THEN ROUND(nrm_gas_hdd::NUMERIC, 1) END) AS day_15
    FROM wdd
    WHERE model = 'WSI'  -- normals are the same across all models; pick one to avoid duplication
),

------------------------------------------------------------
-- COMBINE: Forecast + Observed + Normal
------------------------------------------------------------

combined AS (
    SELECT * FROM forecast_pivot
    UNION ALL
    SELECT * FROM observed_pivot
    UNION ALL
    SELECT * FROM normals_pivot
)

------------------------------------------------------------
-- FINAL OUTPUT: ordered to match reference layout
------------------------------------------------------------

SELECT
    model,
    forecast_execution_datetime,
    cycle,
    type,
    row_type,
    day_01, day_02, day_03, day_04, day_05,
    day_06, day_07, day_08, day_09, day_10,
    day_11, day_12, day_13, day_14, day_15,
    ROUND(
        COALESCE(day_01, 0) + COALESCE(day_02, 0) + COALESCE(day_03, 0) +
        COALESCE(day_04, 0) + COALESCE(day_05, 0) + COALESCE(day_06, 0) +
        COALESCE(day_07, 0) + COALESCE(day_08, 0) + COALESCE(day_09, 0) +
        COALESCE(day_10, 0) + COALESCE(day_11, 0) + COALESCE(day_12, 0) +
        COALESCE(day_13, 0) + COALESCE(day_14, 0) + COALESCE(day_15, 0)
    , 1) AS total
FROM combined
ORDER BY
    CASE row_type
        WHEN 'Forecast' THEN 1
        WHEN 'Observed' THEN 2
        WHEN 'Normal'   THEN 3
    END,
    CASE model
        WHEN 'WSI'             THEN 1
        WHEN 'GFS_OP'          THEN 2
        WHEN 'GFS_ENS'         THEN 3
        WHEN 'GEM_OP'          THEN 4
        WHEN 'GEM_ENS'         THEN 5
        WHEN 'ECMWF_OP'        THEN 6
        WHEN 'ECMWF_ENS'       THEN 7
        WHEN 'ECMWF_AIFS'      THEN 8
        WHEN 'ECMWF_AIFS_ENS'  THEN 9
        WHEN 'ALL'             THEN 99
        ELSE 999
    END,
    cycle;

/*
    ============================================================
    HOW TO VALIDATE AGAINST REFERENCE
    ============================================================

    Source: wsi_cleaned.wdd_daily (combined mart)
    Filters: CONUS region, 15-day forecast window from today

    Output layout:
      Row types:
        1. Forecast — fcst_gas_hdd per model/cycle (compare against WSI Model Forecast view)
        2. Observed — obs_gas_hdd (only today or past will be non-NULL)
        3. Normal   — nrm_gas_hdd (10-year normals for each date)

    Cross-checks:
      - Forecast rows should match validate_wdd_forecasts_daily_v3.sql forecast values
      - Observed day_01 should match wdd_observed_daily for today's date
      - Normal values should be stable day-over-day (only change with the calendar date)

    To change region: replace 'CONUS' in the wdd CTE WHERE clause.
*/
