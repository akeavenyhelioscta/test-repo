-- ercot_lmps_hourly
-- Grain: 1 row per date x hour_ending x hub x market
-- Schema: ercot_cleaned
-- Note: lmp_system_energy_price, lmp_congestion_price, lmp_marginal_loss_price
--       are always NULL because ERCOT only publishes lmp_total.

SELECT
    datetime,                        -- TIMESTAMP  CPT timestamp
    date,                            -- DATE       Operating date
    hour_ending,                     -- INT        Hour ending (1-24)
    hub,                             -- VARCHAR    Pricing hub (HB_HOUSTON, HB_NORTH, HB_SOUTH, HB_WEST)
    market,                          -- VARCHAR    da, rt, or dart
    lmp_total,                       -- NUMERIC    Total LMP ($/MWh)
    lmp_system_energy_price,         -- NUMERIC    Always NULL for ERCOT
    lmp_congestion_price,            -- NUMERIC    Always NULL for ERCOT
    lmp_marginal_loss_price          -- NUMERIC    Always NULL for ERCOT
FROM ercot_cleaned.ercot_lmps_hourly
