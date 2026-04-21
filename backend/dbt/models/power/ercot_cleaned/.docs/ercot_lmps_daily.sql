-- ercot_lmps_daily
-- Grain: 1 row per date
-- Schema: ercot_cleaned

SELECT
    date,                                        -- DATE  Operating date

    -- DA LMPs by hub x period ($/MWh)
    da_lmp_total_houston_hub_flat,
    da_lmp_total_houston_hub_onpeak,
    da_lmp_total_houston_hub_offpeak,
    da_lmp_total_north_hub_flat,
    da_lmp_total_north_hub_onpeak,
    da_lmp_total_north_hub_offpeak,
    da_lmp_total_south_hub_flat,
    da_lmp_total_south_hub_onpeak,
    da_lmp_total_south_hub_offpeak,
    da_lmp_total_west_hub_flat,
    da_lmp_total_west_hub_onpeak,
    da_lmp_total_west_hub_offpeak,

    -- RT LMPs by hub x period ($/MWh)
    rt_lmp_total_houston_hub_flat,
    rt_lmp_total_houston_hub_onpeak,
    rt_lmp_total_houston_hub_offpeak,
    rt_lmp_total_north_hub_flat,
    rt_lmp_total_north_hub_onpeak,
    rt_lmp_total_north_hub_offpeak,
    rt_lmp_total_south_hub_flat,
    rt_lmp_total_south_hub_onpeak,
    rt_lmp_total_south_hub_offpeak,
    rt_lmp_total_west_hub_flat,
    rt_lmp_total_west_hub_onpeak,
    rt_lmp_total_west_hub_offpeak,

    -- DART Spread by hub x period ($/MWh)
    dart_lmp_total_houston_hub_flat,
    dart_lmp_total_houston_hub_onpeak,
    dart_lmp_total_houston_hub_offpeak,
    dart_lmp_total_north_hub_flat,
    dart_lmp_total_north_hub_onpeak,
    dart_lmp_total_north_hub_offpeak,
    dart_lmp_total_south_hub_flat,
    dart_lmp_total_south_hub_onpeak,
    dart_lmp_total_south_hub_offpeak,
    dart_lmp_total_west_hub_flat,
    dart_lmp_total_west_hub_onpeak,
    dart_lmp_total_west_hub_offpeak

FROM ercot_cleaned.ercot_lmps_daily
