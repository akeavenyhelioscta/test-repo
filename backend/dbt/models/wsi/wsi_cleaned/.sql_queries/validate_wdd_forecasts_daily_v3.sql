/*
    VALIDATION v3: wdd_forecasts_daily view
    Compare output against WSI Forecast Views reference (Gas Weighted HDD, CONUS).

    Reads from the mart view wsi_cleaned.wdd_forecasts_daily.
    Pivots 15-day forecast window into day columns matching the reference layout.

    Changes from v2:
      - Added forecast_execution_datetime at the start of the pivot table
      - Added total column (sum of days 1-15) at the end of the pivot table

    Sections:
      1. Forecast rows      - daily gas_hdd per model (matches reference value rows)
      2. Differences rows   - daily gas_hdd_diff_run_over_run per model (matches reference "Differences" rows)
*/

------------------------------------------------------------
-- SECTION 1+2: Forecast values & differences pivoted
------------------------------------------------------------

WITH current_forecast AS (
    SELECT *
    FROM wsi_cleaned.wdd_forecasts_daily
    WHERE forecast_rank = 1
      AND region = 'CONUS'
      AND COALESCE(bias_corrected::TEXT, 'false') = 'false'
      AND forecast_date >= CURRENT_DATE
      AND forecast_date < CURRENT_DATE + 15
),

forecast_pivot AS (
    SELECT
        model,
        COALESCE(cycle, '-') AS cycle,
        MAX(forecast_execution_datetime) AS forecast_execution_datetime,
        'Forecast' AS row_type,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 0  THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_01,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 1  THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_02,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 2  THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_03,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 3  THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_04,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 4  THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_05,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 5  THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_06,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 6  THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_07,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 7  THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_08,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 8  THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_09,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 9  THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_10,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 10 THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_11,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 11 THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_12,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 12 THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_13,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 13 THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_14,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 14 THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_15
    FROM current_forecast
    GROUP BY model, COALESCE(cycle, '-')
),

diff_pivot AS (
    SELECT
        model,
        COALESCE(cycle, '-') AS cycle,
        MAX(forecast_execution_datetime) AS forecast_execution_datetime,
        'Differences' AS row_type,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 0  THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_01,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 1  THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_02,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 2  THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_03,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 3  THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_04,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 4  THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_05,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 5  THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_06,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 6  THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_07,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 7  THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_08,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 8  THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_09,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 9  THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_10,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 10 THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_11,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 11 THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_12,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 12 THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_13,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 13 THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_14,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 14 THEN ROUND(gas_hdd_diff_run_over_run::NUMERIC, 1) END) AS day_15
    FROM current_forecast
    GROUP BY model, COALESCE(cycle, '-')
),

------------------------------------------------------------
-- COMBINE: Forecast + Differences
------------------------------------------------------------

combined AS (
    SELECT * FROM forecast_pivot
    UNION ALL
    SELECT * FROM diff_pivot
)

------------------------------------------------------------
-- FINAL OUTPUT: ordered to match reference layout
------------------------------------------------------------

SELECT
    model,
    forecast_execution_datetime,
    cycle,
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
        ELSE 999
    END,
    cycle,
    CASE row_type
        WHEN 'Forecast'    THEN 1
        WHEN 'Differences' THEN 2
    END;

/*
    ============================================================
    HOW TO VALIDATE AGAINST REFERENCE IMAGE
    ============================================================

    Reference: WSI Forecast Views.png
    Filters:   Gas Weighted HDD | Model Forecast | CONUS | 00Z Fri Mar 06 | Bias Corrected OFF

    Compare each row in the output against:

    MODEL          | ROW TYPE     | REF DAY 15 DIFF
    ---------------|--------------|----------------
    WSI            | Forecast     | (no diff row)
    GFS_OP         | Forecast     |
    GFS_OP         | Differences  | -2.1
    GFS_ENS        | Forecast     |
    GFS_ENS        | Differences  | -0.2
    GEM_OP         | Forecast     |
    GEM_OP         | Differences  | n/a
    GEM_ENS        | Forecast     |
    GEM_ENS        | Differences  | +1.3
    ECMWF_OP       | Forecast     |
    ECMWF_OP       | Differences  | +6.5
    ECMWF_ENS      | Forecast     |
    ECMWF_ENS      | Differences  | -0.4
    ECMWF_AIFS     | Forecast     |
    ECMWF_AIFS     | Differences  | -
    ECMWF_AIFS_ENS | Forecast     |
    ECMWF_AIFS_ENS | Differences  | -
*/
