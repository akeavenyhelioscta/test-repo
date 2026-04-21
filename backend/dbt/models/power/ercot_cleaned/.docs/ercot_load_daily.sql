-- ercot_load_daily
-- Grain: 1 row per date x period
-- Schema: ercot_cleaned

SELECT
    date,                            -- DATE       Operating date
    period,                          -- TEXT       flat / onpeak / offpeak

    -- Load by zone (avg MW)
    load_total,
    load_north,
    load_south,
    load_west,
    load_houston

FROM ercot_cleaned.ercot_load_daily
