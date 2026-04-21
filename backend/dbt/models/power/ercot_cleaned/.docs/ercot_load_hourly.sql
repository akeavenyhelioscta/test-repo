-- ercot_load_hourly
-- Grain: 1 row per date x hour_ending
-- Schema: ercot_cleaned

SELECT
    interval_start,                  -- TIMESTAMP  CPT timestamp
    date,                            -- DATE       Operating date
    hour_ending,                     -- INT        Hour ending (1-24)

    -- Load by zone (MW)
    load_north,
    load_south,
    load_west,
    load_houston,
    load_total                       -- Sum of all zones

FROM ercot_cleaned.ercot_load_hourly
