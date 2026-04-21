-- ercot_fuel_mix_hourly
-- Grain: 1 row per date x hour_ending
-- Schema: ercot_cleaned

SELECT
    datetime,                        -- TIMESTAMP  CPT timestamp
    date,                            -- DATE       Operating date
    hour_ending,                     -- INT        Hour ending (1-24)

    -- Fuel types (MW)
    nuclear,
    hydro,
    wind,
    solar,
    natural_gas,
    coal_and_lignite,
    power_storage,
    other,

    -- Energy storage (MW)
    storage_net_output,
    storage_total_charging,
    storage_total_discharging,

    -- Derived (MW)
    total,                           -- Sum of all fuel types
    renewables,                      -- wind + solar
    thermal,                         -- natural_gas + coal_and_lignite

    -- Thermal share (ratio)
    gas_pct_thermal,
    coal_pct_thermal

FROM ercot_cleaned.ercot_fuel_mix_hourly
