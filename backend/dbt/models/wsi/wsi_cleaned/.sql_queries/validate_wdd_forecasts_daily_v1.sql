/*
    VALIDATION v1: wdd_forecasts_daily view
    Compare output against WSI Forecast Views reference (Gas Weighted HDD, CONUS, 10yr Avg).

    Reads from the NEW mart view wsi_cleaned.wdd_forecasts_daily.
    Pivots 15-day forecast window into day columns matching the reference layout.

    Sections:
      1. Forecast rows   - daily gas_hdd per model (matches reference value rows)
      2. Differences rows - daily gas_hdd_diff per model (matches reference "Differences" rows)
      3. Totals           - gas_hdd_total per model (matches reference "Total" column)
      4. Diff totals      - sum of daily diffs per model (matches reference diff "Total" column)
      5. Day 15 diffs     - gas_hdd_diff on day 15 per model (explicit Day 15 check)
      6. Normals row      - 10yr normal from wdd_normals_daily (matches reference bottom row)
*/

------------------------------------------------------------
-- SECTION 1+2: Forecast values & differences pivoted
------------------------------------------------------------

WITH current_forecast AS (
    SELECT *
    FROM wsi_cleaned.wdd_forecasts_daily
    WHERE rank_forecast_execution_timestamps = 1
      AND region = 'CONUS'
      AND COALESCE(bias_corrected::TEXT, 'false') = 'false'
      AND forecast_date >= CURRENT_DATE
      AND forecast_date < CURRENT_DATE + 15
),

forecast_pivot AS (
    SELECT
        model,
        COALESCE(cycle, '-') AS cycle,
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
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 14 THEN ROUND(gas_hdd::NUMERIC, 1) END) AS day_15,
        ROUND(MAX(gas_hdd_total::NUMERIC), 1) AS total,
        ROUND(MAX(gas_hdd_diff_total::NUMERIC), 1) AS diff_total
    FROM current_forecast
    GROUP BY model, COALESCE(cycle, '-')
),

diff_pivot_base AS (
    SELECT
        model,
        COALESCE(cycle, '-') AS cycle,
        'Differences' AS row_type,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 0  THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_01,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 1  THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_02,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 2  THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_03,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 3  THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_04,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 4  THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_05,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 5  THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_06,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 6  THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_07,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 7  THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_08,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 8  THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_09,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 9  THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_10,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 10 THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_11,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 11 THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_12,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 12 THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_13,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 13 THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_14,
        MAX(CASE WHEN forecast_date = CURRENT_DATE + 14 THEN ROUND(gas_hdd_diff::NUMERIC, 1) END) AS day_15
    FROM current_forecast
    GROUP BY model, COALESCE(cycle, '-')
),

diff_pivot AS (
    SELECT
        model,
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
            COALESCE(day_13, 0) + COALESCE(day_14, 0) + COALESCE(day_15, 0),
            1
        ) AS total,
        NULL::NUMERIC AS diff_total
    FROM diff_pivot_base
),

------------------------------------------------------------
-- SECTION 6: Normals (10yr) from wdd_normals_daily
------------------------------------------------------------

normals_pivot AS (
    SELECT
        'NORMAL_10YR' AS model,
        '-' AS cycle,
        'Normals' AS row_type,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 0,  'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_01,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 1,  'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_02,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 2,  'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_03,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 3,  'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_04,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 4,  'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_05,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 5,  'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_06,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 6,  'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_07,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 7,  'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_08,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 8,  'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_09,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 9,  'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_10,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 10, 'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_11,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 11, 'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_12,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 12, 'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_13,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 13, 'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_14,
        MAX(CASE WHEN mm_dd = TO_CHAR(CURRENT_DATE + 14, 'MM-DD') THEN ROUND(gas_hdd_normal::NUMERIC, 1) END) AS day_15,
        ROUND(SUM(gas_hdd_normal::NUMERIC), 1) AS total,
        NULL::NUMERIC AS diff_total
    FROM wsi_cleaned.wdd_normals_daily
    WHERE region = 'CONUS'
      AND period = '10_year'
      AND mm_dd IN (
        TO_CHAR(CURRENT_DATE + 0,  'MM-DD'), TO_CHAR(CURRENT_DATE + 1,  'MM-DD'),
        TO_CHAR(CURRENT_DATE + 2,  'MM-DD'), TO_CHAR(CURRENT_DATE + 3,  'MM-DD'),
        TO_CHAR(CURRENT_DATE + 4,  'MM-DD'), TO_CHAR(CURRENT_DATE + 5,  'MM-DD'),
        TO_CHAR(CURRENT_DATE + 6,  'MM-DD'), TO_CHAR(CURRENT_DATE + 7,  'MM-DD'),
        TO_CHAR(CURRENT_DATE + 8,  'MM-DD'), TO_CHAR(CURRENT_DATE + 9,  'MM-DD'),
        TO_CHAR(CURRENT_DATE + 10, 'MM-DD'), TO_CHAR(CURRENT_DATE + 11, 'MM-DD'),
        TO_CHAR(CURRENT_DATE + 12, 'MM-DD'), TO_CHAR(CURRENT_DATE + 13, 'MM-DD'),
        TO_CHAR(CURRENT_DATE + 14, 'MM-DD')
      )
),

------------------------------------------------------------
-- COMBINE: Forecast + Differences + Normals
------------------------------------------------------------

combined AS (
    SELECT * FROM forecast_pivot
    UNION ALL
    SELECT * FROM diff_pivot
    UNION ALL
    SELECT * FROM normals_pivot
)

------------------------------------------------------------
-- FINAL OUTPUT: ordered to match reference layout
------------------------------------------------------------

SELECT
    model,
    cycle,
    row_type,
    day_01, day_02, day_03, day_04, day_05,
    day_06, day_07, day_08, day_09, day_10,
    day_11, day_12, day_13, day_14, day_15,
    total,
    diff_total
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
        WHEN 'NORMAL_10YR'     THEN 10
        ELSE 999
    END,
    cycle,
    CASE row_type
        WHEN 'Forecast'    THEN 1
        WHEN 'Differences' THEN 2
        WHEN 'Normals'     THEN 3
    END;

/*
    ============================================================
    HOW TO VALIDATE AGAINST REFERENCE IMAGE
    ============================================================

    Reference: WSI Forecast Views.png
    Filters:   Gas Weighted HDD | Model Forecast | 10yr Avg | CONUS | 00Z Fri Mar 06 | Bias Corrected OFF

    Compare each row in the output against:

    MODEL          | ROW TYPE     | REF TOTAL | REF DAY 15 DIFF
    ---------------|--------------|-----------|----------------
    WSI            | Forecast     | 231.1     | (no diff row)
    GFS_OP         | Forecast     | 244.7     |
    GFS_OP         | Differences  | +12.0     | -2.1
    GFS_ENS        | Forecast     | 232.0     |
    GFS_ENS        | Differences  | -1.9      | -0.2
    GEM_OP         | Forecast     | 190.0     |
    GEM_OP         | Differences  | +10.7     | n/a
    GEM_ENS        | Forecast     | 280.9     |
    GEM_ENS        | Differences  | +5.0      | +1.3
    ECMWF_OP       | Forecast     | 268.9     |
    ECMWF_OP       | Differences  | -9.8      | +6.5
    ECMWF_ENS      | Forecast     | 265.2     |
    ECMWF_ENS      | Differences  | -8.6      | -0.4
    ECMWF_AIFS     | Forecast     | 247.5     |
    ECMWF_AIFS     | Differences  | +2.6      | -
    ECMWF_AIFS_ENS | Forecast     | 254.0     |
    ECMWF_AIFS_ENS | Differences  | +1.8      | -
    NORMAL_10YR    | Normals      | 288.5     | (n/a)

    KNOWN ISSUES in wdd_forecasts_daily.sql (pre-fix):
    1. gas_hdd_diff_total column missing - diff totals won't appear
    2. WSI totals are NULL - WSI total will be NULL instead of 231.1
    3. WSI normals/diffs/departures NULL - staging provides real values
    4. Staging model filter excludes GEM_OP, GEM_ENS, ECMWF_AIFS, ECMWF_AIFS_ENS
*/
