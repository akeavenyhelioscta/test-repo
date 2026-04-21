-------------------------------------------------------------
-- Validate: genscape_gas_production_forecast_monthly
-- Compare against Spring Rock PDF report (As of Mar 6, 2026)
-------------------------------------------------------------

-- 1. Verify only official monthly reportDates are present (no weekly interim dates)
--    Expected: 9 dates | Must NOT contain: 2026-03-20, 2026-03-13, 2025-09-26
SELECT 'monthly_report_dates' as check_name, report_date
FROM genscape_cleaned.genscape_gas_production_forecast_monthly
GROUP BY report_date
ORDER BY report_date DESC;


-- 2. Verify weekly view still has ALL reportDates (12 total)
SELECT 'weekly_report_dates' as check_name, report_date
FROM genscape_cleaned.genscape_gas_production_forecast_weekly
GROUP BY report_date
ORDER BY report_date DESC;


-- 3. Validate Lower 48 values against PDF report (report_date = 2026-03-06)
--    Source: Spring Rock Natural Gas Production Forecast, Page 2, "Monthly Data"
--
--    | Lower 48   | Jan-26  | Feb-26  | Mar-26  | Apr-26  | May-26  | Jun-26  |
--    |------------|---------|---------|---------|---------|---------|---------|
--    | Dry Prod   | 106,854 | 109,284 | 110,283 | 110,952 | 111,136 | 111,478 |
--    | Yr/Yr      |   4,728 |   5,854 |   5,294 |   5,476 |   5,332 |   4,618 |
--    | Gas Rigs   |     134 |     137 |     140 |     143 |     143 |     140 |
--    | Oil Rigs   |     396 |     392 |     392 |     392 |     396 |     408 |
SELECT
    date
    ,EXTRACT(MONTH FROM date) as month
    ,lower_48_production
    ,lower_48_dry_gas_production_yoy
    ,lower_48_gas_rig_count
    ,lower_48_oil_rig_count
FROM genscape_cleaned.genscape_gas_production_forecast_monthly
WHERE report_date = '2026-03-06'
  AND date >= '2026-01-01'
  AND date <= '2026-12-31'
ORDER BY date;


-- 4. Validate Western Canada values against PDF report (report_date = 2026-03-06)
--
--    | W. Canada  | Jan-26  | Feb-26  | Mar-26  | Apr-26  | May-26  | Jun-26  |
--    |------------|---------|---------|---------|---------|---------|---------|
--    | Dry Prod   |  19,841 |  19,661 |  20,090 |  20,323 |  20,227 |  20,144 |
--    | Yr/Yr      |     632 |     766 |     823 |   1,333 |   1,267 |   1,709 |
--    | Gas Rigs   |      69 |      71 |      61 |      49 |      45 |      46 |
--    | Oil Rigs   |     124 |     148 |     107 |      48 |      50 |      58 |
SELECT
    date
    ,EXTRACT(MONTH FROM date) as month
    ,western_canada_production
    ,western_canada_dry_gas_production_yoy
    ,western_canada_gas_rig_count
    ,western_canada_oil_rig_count
FROM genscape_cleaned.genscape_gas_production_forecast_monthly
WHERE report_date = '2026-03-06'
  AND date >= '2026-01-01'
  AND date <= '2026-12-31'
ORDER BY date;


-- 5. Verify revision numbers are recalculated for monthly only
--    For a given date, max_revision in monthly should be <= max_revision in weekly
SELECT
    m.date
    ,m.report_date
    ,m.revision as monthly_revision
    ,m.max_revision as monthly_max_revision
    ,w.revision as weekly_revision
    ,w.max_revision as weekly_max_revision
FROM genscape_cleaned.genscape_gas_production_forecast_monthly m
JOIN genscape_cleaned.genscape_gas_production_forecast_weekly w
  ON m.date = w.date AND m.report_date = w.report_date
WHERE m.date = '2026-06-01'
ORDER BY m.report_date;


-- 6. Confirm no interim dates leaked into monthly view
SELECT 'leaked_interim_dates' as check_name, COUNT(*) as count
FROM genscape_cleaned.genscape_gas_production_forecast_monthly
WHERE report_date IN ('2026-03-20', '2026-03-13', '2025-09-26');
