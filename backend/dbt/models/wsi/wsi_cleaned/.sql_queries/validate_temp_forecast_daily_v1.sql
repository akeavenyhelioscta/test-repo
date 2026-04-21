/*
    VALIDATION v1: temp_forecast_daily + temp_observed_daily
    Compare output against AG2 Trader Temperature > Min/Max tab (reference: TEMP-WSI-Forecast-Table.png).

    Layout:
      - Rows: one per station, grouped by region (default PJM)
      - Columns: 3 observed days (D-3, D-2, D-1) + today + 14 forecast days + normals 15-day avg
      - Values: min_temp / max_temp per day
      - Below each station: difference row (forecast minus 30yr normal)

    Reads from:
      - wsi_cleaned.temp_forecast_daily   (current forecast, min/max/avg per station per day)
      - wsi_cleaned.temp_observed_daily   (observed min/max/avg per station per day)

    To change region: replace 'PJM' in the forecast/normals WHERE clauses below.

    NOTE: temp_observed_daily uses raw WSI regions (CONUS, EAST, MIDWEST, etc.)
    while temp_forecast_daily uses ISO regions (PJM, ERCOT, etc.) from the
    wsi_trader_city_ids seed. Observed data is joined via site_id to the
    forecast stations — no region filter needed on observed.
*/

------------------------------------------------------------
-- SECTION 1: Forecast site_ids for the target region
------------------------------------------------------------

WITH forecast_sites AS (
    SELECT DISTINCT site_id
    FROM wsi_cleaned.temp_forecast_daily
    WHERE region = 'PJM'
),

------------------------------------------------------------
-- SECTION 2: Observed (last 3 days) — joined by site_id
------------------------------------------------------------

observed AS (
    SELECT
        o.station_name,
        o.site_id,
        o.date,
        o.temp_min,
        o.temp_max,
        o.temp
    FROM wsi_cleaned.temp_observed_daily o
    INNER JOIN forecast_sites fs ON o.site_id = fs.site_id
    WHERE o.date >= CURRENT_DATE - 3
      AND o.date < CURRENT_DATE
),

------------------------------------------------------------
-- SECTION 3: Current forecast (15 days from today)
------------------------------------------------------------

forecast AS (
    SELECT
        station_name,
        site_id,
        forecast_date AS date,
        min_temp AS temp_min,
        max_temp AS temp_max,
        avg_temp AS temp
    FROM wsi_cleaned.temp_forecast_daily
    WHERE forecast_date >= CURRENT_DATE
      AND forecast_date < CURRENT_DATE + 15
      AND region = 'PJM'
),

------------------------------------------------------------
-- SECTION 4: Pivot observed into day columns
------------------------------------------------------------

observed_pivot AS (
    SELECT
        station_name,
        site_id,
        'Observed' AS row_type,
        MAX(CASE WHEN date = CURRENT_DATE - 3 THEN ROUND(temp_min::NUMERIC, 0) END) AS obs_d3_min,
        MAX(CASE WHEN date = CURRENT_DATE - 3 THEN ROUND(temp_max::NUMERIC, 0) END) AS obs_d3_max,
        MAX(CASE WHEN date = CURRENT_DATE - 2 THEN ROUND(temp_min::NUMERIC, 0) END) AS obs_d2_min,
        MAX(CASE WHEN date = CURRENT_DATE - 2 THEN ROUND(temp_max::NUMERIC, 0) END) AS obs_d2_max,
        MAX(CASE WHEN date = CURRENT_DATE - 1 THEN ROUND(temp_min::NUMERIC, 0) END) AS obs_d1_min,
        MAX(CASE WHEN date = CURRENT_DATE - 1 THEN ROUND(temp_max::NUMERIC, 0) END) AS obs_d1_max
    FROM observed
    GROUP BY station_name, site_id
),

------------------------------------------------------------
-- SECTION 5: Pivot forecast into day columns
------------------------------------------------------------

forecast_pivot AS (
    SELECT
        station_name,
        site_id,
        'Forecast' AS row_type,
        MAX(CASE WHEN date = CURRENT_DATE + 0  THEN ROUND(temp_min::NUMERIC, 0) END) AS day_01_min,
        MAX(CASE WHEN date = CURRENT_DATE + 0  THEN ROUND(temp_max::NUMERIC, 0) END) AS day_01_max,
        MAX(CASE WHEN date = CURRENT_DATE + 1  THEN ROUND(temp_min::NUMERIC, 0) END) AS day_02_min,
        MAX(CASE WHEN date = CURRENT_DATE + 1  THEN ROUND(temp_max::NUMERIC, 0) END) AS day_02_max,
        MAX(CASE WHEN date = CURRENT_DATE + 2  THEN ROUND(temp_min::NUMERIC, 0) END) AS day_03_min,
        MAX(CASE WHEN date = CURRENT_DATE + 2  THEN ROUND(temp_max::NUMERIC, 0) END) AS day_03_max,
        MAX(CASE WHEN date = CURRENT_DATE + 3  THEN ROUND(temp_min::NUMERIC, 0) END) AS day_04_min,
        MAX(CASE WHEN date = CURRENT_DATE + 3  THEN ROUND(temp_max::NUMERIC, 0) END) AS day_04_max,
        MAX(CASE WHEN date = CURRENT_DATE + 4  THEN ROUND(temp_min::NUMERIC, 0) END) AS day_05_min,
        MAX(CASE WHEN date = CURRENT_DATE + 4  THEN ROUND(temp_max::NUMERIC, 0) END) AS day_05_max,
        MAX(CASE WHEN date = CURRENT_DATE + 5  THEN ROUND(temp_min::NUMERIC, 0) END) AS day_06_min,
        MAX(CASE WHEN date = CURRENT_DATE + 5  THEN ROUND(temp_max::NUMERIC, 0) END) AS day_06_max,
        MAX(CASE WHEN date = CURRENT_DATE + 6  THEN ROUND(temp_min::NUMERIC, 0) END) AS day_07_min,
        MAX(CASE WHEN date = CURRENT_DATE + 6  THEN ROUND(temp_max::NUMERIC, 0) END) AS day_07_max,
        MAX(CASE WHEN date = CURRENT_DATE + 7  THEN ROUND(temp_min::NUMERIC, 0) END) AS day_08_min,
        MAX(CASE WHEN date = CURRENT_DATE + 7  THEN ROUND(temp_max::NUMERIC, 0) END) AS day_08_max,
        MAX(CASE WHEN date = CURRENT_DATE + 8  THEN ROUND(temp_min::NUMERIC, 0) END) AS day_09_min,
        MAX(CASE WHEN date = CURRENT_DATE + 8  THEN ROUND(temp_max::NUMERIC, 0) END) AS day_09_max,
        MAX(CASE WHEN date = CURRENT_DATE + 9  THEN ROUND(temp_min::NUMERIC, 0) END) AS day_10_min,
        MAX(CASE WHEN date = CURRENT_DATE + 9  THEN ROUND(temp_max::NUMERIC, 0) END) AS day_10_max,
        MAX(CASE WHEN date = CURRENT_DATE + 10 THEN ROUND(temp_min::NUMERIC, 0) END) AS day_11_min,
        MAX(CASE WHEN date = CURRENT_DATE + 10 THEN ROUND(temp_max::NUMERIC, 0) END) AS day_11_max,
        MAX(CASE WHEN date = CURRENT_DATE + 11 THEN ROUND(temp_min::NUMERIC, 0) END) AS day_12_min,
        MAX(CASE WHEN date = CURRENT_DATE + 11 THEN ROUND(temp_max::NUMERIC, 0) END) AS day_12_max,
        MAX(CASE WHEN date = CURRENT_DATE + 12 THEN ROUND(temp_min::NUMERIC, 0) END) AS day_13_min,
        MAX(CASE WHEN date = CURRENT_DATE + 12 THEN ROUND(temp_max::NUMERIC, 0) END) AS day_13_max,
        MAX(CASE WHEN date = CURRENT_DATE + 13 THEN ROUND(temp_min::NUMERIC, 0) END) AS day_14_min,
        MAX(CASE WHEN date = CURRENT_DATE + 13 THEN ROUND(temp_max::NUMERIC, 0) END) AS day_14_max,
        MAX(CASE WHEN date = CURRENT_DATE + 14 THEN ROUND(temp_min::NUMERIC, 0) END) AS day_15_min,
        MAX(CASE WHEN date = CURRENT_DATE + 14 THEN ROUND(temp_max::NUMERIC, 0) END) AS day_15_max
    FROM forecast
    GROUP BY station_name, site_id
),

------------------------------------------------------------
-- SECTION 6: 30yr normals average (15-day window)
------------------------------------------------------------

normals_avg AS (
    SELECT
        o.station_name,
        o.site_id,
        ROUND(AVG(o.temp_min::NUMERIC), 0) AS normals_15d_min,
        ROUND(AVG(o.temp_max::NUMERIC), 0) AS normals_15d_max
    FROM wsi_cleaned.temp_observed_daily o
    INNER JOIN forecast_sites fs ON o.site_id = fs.site_id
    WHERE EXTRACT(YEAR FROM o.date) >= EXTRACT(YEAR FROM CURRENT_DATE) - 30
      AND TO_CHAR(o.date, 'MM-DD') IN (
          TO_CHAR(CURRENT_DATE + 0,  'MM-DD'), TO_CHAR(CURRENT_DATE + 1,  'MM-DD'),
          TO_CHAR(CURRENT_DATE + 2,  'MM-DD'), TO_CHAR(CURRENT_DATE + 3,  'MM-DD'),
          TO_CHAR(CURRENT_DATE + 4,  'MM-DD'), TO_CHAR(CURRENT_DATE + 5,  'MM-DD'),
          TO_CHAR(CURRENT_DATE + 6,  'MM-DD'), TO_CHAR(CURRENT_DATE + 7,  'MM-DD'),
          TO_CHAR(CURRENT_DATE + 8,  'MM-DD'), TO_CHAR(CURRENT_DATE + 9,  'MM-DD'),
          TO_CHAR(CURRENT_DATE + 10, 'MM-DD'), TO_CHAR(CURRENT_DATE + 11, 'MM-DD'),
          TO_CHAR(CURRENT_DATE + 12, 'MM-DD'), TO_CHAR(CURRENT_DATE + 13, 'MM-DD'),
          TO_CHAR(CURRENT_DATE + 14, 'MM-DD')
      )
    GROUP BY o.station_name, o.site_id
),

------------------------------------------------------------
-- SECTION 7: Combine observed + forecast + normals
------------------------------------------------------------

combined AS (
    SELECT
        f.station_name,
        f.site_id,

        -- -- 3 days observed (min/max)
        -- o.obs_d3_min, o.obs_d3_max,
        -- o.obs_d2_min, o.obs_d2_max,
        -- o.obs_d1_min, o.obs_d1_max,

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
        f.day_15_min, f.day_15_max,

        -- normals 15-day avg
        n.normals_15d_min,
        n.normals_15d_max

    FROM forecast_pivot f
    LEFT JOIN observed_pivot o ON f.site_id = o.site_id
    LEFT JOIN normals_avg n ON f.site_id = n.site_id
)

------------------------------------------------------------
-- FINAL OUTPUT
------------------------------------------------------------

SELECT *
FROM combined
ORDER BY station_name;

/*
    ============================================================
    HOW TO VALIDATE AGAINST REFERENCE IMAGE
    ============================================================

    Reference: TEMP-WSI-Forecast-Table.png
    Filters:   Temperature > Min/Max | Forecast Values | vs 30yr Avg | PJM | Tue Mar 17 Update

    For each station row, compare:
      1. obs_d3/d2/d1 columns → should match observed actuals for the prior 3 days
      2. day_01 through day_15 → should match the 15-day min/max forecast values
      3. normals_15d → should approximate the "Normals 15 Day Avg" column

    Example from screenshot (PJM agg, Mar 17):
      day_01: 25/39, day_02: 23/41, day_03: 32/51, day_04: 37/59, ...
      normals_15d: 36/55

    To change region: replace 'PJM' in the forecast WHERE clause (Section 3).
*/
