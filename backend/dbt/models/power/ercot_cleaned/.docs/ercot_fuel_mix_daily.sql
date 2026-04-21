-- ercot_fuel_mix_daily
-- Grain: 1 row per date x period
-- Schema: ercot_cleaned

SELECT
    date,                            -- DATE       Operating date
    period,                          -- TEXT       flat / onpeak / offpeak

    -- Fuel types (avg MW)
    nuclear,
    hydro,
    wind,
    solar,
    natural_gas,
    coal_and_lignite,
    power_storage,
    other,

    -- Energy storage (avg MW)
    storage_net_output,
    storage_total_charging,
    storage_total_discharging,

    -- Derived (avg MW)
    total,
    renewables,
    thermal,
    gas_pct_thermal,
    coal_pct_thermal

FROM ercot_cleaned.ercot_fuel_mix_daily
