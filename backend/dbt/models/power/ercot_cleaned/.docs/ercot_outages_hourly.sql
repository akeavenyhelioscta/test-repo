-- ercot_outages_hourly
-- Grain: 1 row per date x hour_ending
-- Schema: ercot_cleaned

SELECT
    date,                            -- DATE       Operating date
    hour_ending,                     -- INT        Hour ending (1-24)

    -- Outages (avg MW)
    combined_unplanned,              -- Unplanned outages (total - planned)
    combined_planned,                -- Planned outages
    combined_total                   -- Total outages across all zones

FROM ercot_cleaned.ercot_outages_hourly
