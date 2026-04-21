-- isone_lmps_daily
-- Grain: 1 row per date
-- Schema: isone_cleaned

SELECT
    date,                                        -- DATE  Operating date

    -- DA LMPs by period ($/MWh)
    da_lmp_total_internal_hub_flat,
    da_lmp_total_internal_hub_onpeak,
    da_lmp_total_internal_hub_offpeak,
    da_lmp_system_energy_price_internal_hub_flat,
    da_lmp_system_energy_price_internal_hub_onpeak,
    da_lmp_system_energy_price_internal_hub_offpeak,
    da_lmp_congestion_price_internal_hub_flat,
    da_lmp_congestion_price_internal_hub_onpeak,
    da_lmp_congestion_price_internal_hub_offpeak,
    da_lmp_marginal_loss_price_internal_hub_flat,
    da_lmp_marginal_loss_price_internal_hub_onpeak,
    da_lmp_marginal_loss_price_internal_hub_offpeak,

    -- RT LMPs by period ($/MWh)
    rt_lmp_total_internal_hub_flat,
    rt_lmp_total_internal_hub_onpeak,
    rt_lmp_total_internal_hub_offpeak,
    rt_lmp_system_energy_price_internal_hub_flat,
    rt_lmp_system_energy_price_internal_hub_onpeak,
    rt_lmp_system_energy_price_internal_hub_offpeak,
    rt_lmp_congestion_price_internal_hub_flat,
    rt_lmp_congestion_price_internal_hub_onpeak,
    rt_lmp_congestion_price_internal_hub_offpeak,
    rt_lmp_marginal_loss_price_internal_hub_flat,
    rt_lmp_marginal_loss_price_internal_hub_onpeak,
    rt_lmp_marginal_loss_price_internal_hub_offpeak,

    -- DART Spread by period ($/MWh)
    dart_lmp_total_internal_hub_flat,
    dart_lmp_total_internal_hub_onpeak,
    dart_lmp_total_internal_hub_offpeak,
    dart_lmp_system_energy_price_internal_hub_flat,
    dart_lmp_system_energy_price_internal_hub_onpeak,
    dart_lmp_system_energy_price_internal_hub_offpeak,
    dart_lmp_congestion_price_internal_hub_flat,
    dart_lmp_congestion_price_internal_hub_onpeak,
    dart_lmp_congestion_price_internal_hub_offpeak,
    dart_lmp_marginal_loss_price_internal_hub_flat,
    dart_lmp_marginal_loss_price_internal_hub_onpeak,
    dart_lmp_marginal_loss_price_internal_hub_offpeak

FROM isone_cleaned.isone_lmps_daily
