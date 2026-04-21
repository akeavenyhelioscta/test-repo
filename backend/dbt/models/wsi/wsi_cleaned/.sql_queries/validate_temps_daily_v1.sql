/*
    VALIDATION v1: temps_daily combined mart
    Compare output against AG2 Trader Temperature > Min/Max tab (reference: TEMP-WSI-Forecast-Table.png).

    Reads from the combined mart view wsi_cleaned.temps_daily which joins:
      - Latest forecast (rank 1) from source_v1_weighted_daily_forecast_temp_city
      - Observed from temp_observed_daily

    Layout:
      - Rows: one per station, grouped by region (default PJM)
      - Columns: 3 observed days (D-3, D-2, D-1) + 15 forecast days (today + 14)
      - Values: min_temp / max_temp per day

    To change region: replace 'PJM' in the WHERE clause below.
*/

------------------------------------------------------------
-- SECTION 1: Filter to target region + 15-day forecast window
------------------------------------------------------------

WITH temps AS (
    SELECT *
    FROM wsi_cleaned.temps_daily
    WHERE region = 'PJM'
      AND date >= CURRENT_DATE
      AND date < CURRENT_DATE + 15
),

------------------------------------------------------------
-- SECTION 2: Observed for the prior 3 days (joined by site_id)
------------------------------------------------------------

observed_past AS (
    SELECT
        t.site_id,
        t.station_name,
        o.date,
        o.obs_temp_min,
        o.obs_temp_max,
        o.obs_temp
    FROM (SELECT DISTINCT site_id, station_name FROM temps) t
    INNER JOIN wsi_cleaned.temps_daily o
        ON t.site_id = o.site_id
        AND o.date >= CURRENT_DATE - 3
        AND o.date < CURRENT_DATE
        AND o.region = 'PJM'
),

------------------------------------------------------------
-- SECTION 3: Pivot observed into day columns (D-3, D-2, D-1)
------------------------------------------------------------

observed_pivot AS (
    SELECT
        station_name,
        site_id,
        MAX(CASE WHEN date = CURRENT_DATE - 3 THEN ROUND(obs_temp_min::NUMERIC, 0) END) AS obs_d3_min,
        MAX(CASE WHEN date = CURRENT_DATE - 3 THEN ROUND(obs_temp_max::NUMERIC, 0) END) AS obs_d3_max,
        MAX(CASE WHEN date = CURRENT_DATE - 2 THEN ROUND(obs_temp_min::NUMERIC, 0) END) AS obs_d2_min,
        MAX(CASE WHEN date = CURRENT_DATE - 2 THEN ROUND(obs_temp_max::NUMERIC, 0) END) AS obs_d2_max,
        MAX(CASE WHEN date = CURRENT_DATE - 1 THEN ROUND(obs_temp_min::NUMERIC, 0) END) AS obs_d1_min,
        MAX(CASE WHEN date = CURRENT_DATE - 1 THEN ROUND(obs_temp_max::NUMERIC, 0) END) AS obs_d1_max
    FROM observed_past
    GROUP BY station_name, site_id
),

------------------------------------------------------------
-- SECTION 4: Pivot forecast into 15 day columns (min/max)
------------------------------------------------------------

forecast_pivot AS (
    SELECT
        station_name,
        site_id,
        MAX(forecast_execution_datetime) AS forecast_execution_datetime,
        MAX(CASE WHEN date = CURRENT_DATE + 0  THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_01_min,
        MAX(CASE WHEN date = CURRENT_DATE + 0  THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_01_max,
        MAX(CASE WHEN date = CURRENT_DATE + 1  THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_02_min,
        MAX(CASE WHEN date = CURRENT_DATE + 1  THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_02_max,
        MAX(CASE WHEN date = CURRENT_DATE + 2  THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_03_min,
        MAX(CASE WHEN date = CURRENT_DATE + 2  THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_03_max,
        MAX(CASE WHEN date = CURRENT_DATE + 3  THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_04_min,
        MAX(CASE WHEN date = CURRENT_DATE + 3  THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_04_max,
        MAX(CASE WHEN date = CURRENT_DATE + 4  THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_05_min,
        MAX(CASE WHEN date = CURRENT_DATE + 4  THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_05_max,
        MAX(CASE WHEN date = CURRENT_DATE + 5  THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_06_min,
        MAX(CASE WHEN date = CURRENT_DATE + 5  THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_06_max,
        MAX(CASE WHEN date = CURRENT_DATE + 6  THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_07_min,
        MAX(CASE WHEN date = CURRENT_DATE + 6  THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_07_max,
        MAX(CASE WHEN date = CURRENT_DATE + 7  THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_08_min,
        MAX(CASE WHEN date = CURRENT_DATE + 7  THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_08_max,
        MAX(CASE WHEN date = CURRENT_DATE + 8  THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_09_min,
        MAX(CASE WHEN date = CURRENT_DATE + 8  THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_09_max,
        MAX(CASE WHEN date = CURRENT_DATE + 9  THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_10_min,
        MAX(CASE WHEN date = CURRENT_DATE + 9  THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_10_max,
        MAX(CASE WHEN date = CURRENT_DATE + 10 THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_11_min,
        MAX(CASE WHEN date = CURRENT_DATE + 10 THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_11_max,
        MAX(CASE WHEN date = CURRENT_DATE + 11 THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_12_min,
        MAX(CASE WHEN date = CURRENT_DATE + 11 THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_12_max,
        MAX(CASE WHEN date = CURRENT_DATE + 12 THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_13_min,
        MAX(CASE WHEN date = CURRENT_DATE + 12 THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_13_max,
        MAX(CASE WHEN date = CURRENT_DATE + 13 THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_14_min,
        MAX(CASE WHEN date = CURRENT_DATE + 13 THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_14_max,
        MAX(CASE WHEN date = CURRENT_DATE + 14 THEN ROUND(fcst_min_temp::NUMERIC, 0) END) AS day_15_min,
        MAX(CASE WHEN date = CURRENT_DATE + 14 THEN ROUND(fcst_max_temp::NUMERIC, 0) END) AS day_15_max
    FROM temps
    GROUP BY station_name, site_id
),

------------------------------------------------------------
-- SECTION 5: Combine observed + forecast
------------------------------------------------------------

combined AS (
    SELECT
        f.station_name,
        f.site_id,
        f.forecast_execution_datetime,

        -- 3 days observed (min/max)
        o.obs_d3_min, o.obs_d3_max,
        o.obs_d2_min, o.obs_d2_max,
        o.obs_d1_min, o.obs_d1_max,

        -- 15 days forecast (min/max)
        f.day_01_min, f.day_01_max,
        f.day_02_min, f.day_02_max,
        f.day_03_min, f.day_03_max,
        f.day_04_min, f.day_04_max,
        f.day_05_min, f.day_05_max,
        f.day_06_min, f.day_06_max,
        f.day_07_min, f.day_07_max,
        f.day_08_min, f.day_08_max,
        f.day_09_min, f.day_09_max,
        f.day_10_min, f.day_10_max,
        f.day_11_min, f.day_11_max,
        f.day_12_min, f.day_12_max,
        f.day_13_min, f.day_13_max,
        f.day_14_min, f.day_14_max,
        f.day_15_min, f.day_15_max

    FROM forecast_pivot f
    LEFT JOIN observed_pivot o ON f.site_id = o.site_id
)

------------------------------------------------------------
-- FINAL OUTPUT
------------------------------------------------------------

SELECT *
FROM combined
ORDER BY station_name;

/*
    ============================================================
    HOW TO VALIDATE AGAINST REFERENCE
    ============================================================

    Source: wsi_cleaned.temps_daily (combined mart)
    Reference: TEMP-WSI-Forecast-Table.png
    Filters: Temperature > Min/Max | PJM region

    Cross-checks:
      - obs_d3/d2/d1 columns → should match observed min/max for the prior 3 days
        (these come from the obs_temp_min / obs_temp_max columns in temps_daily)
      - day_01 through day_15 → should match the 15-day forecast min/max values
        (these come from fcst_min_temp / fcst_max_temp in temps_daily)
      - Forecast values should match validate_temp_forecast_daily_v1.sql output

    Example cross-check (PJM, Mar 17):
      day_01: 25/39, day_02: 23/41, day_03: 32/51, day_04: 37/59, ...

    To change region: replace 'PJM' in the temps CTE and observed_past CTE WHERE clauses.
*/
