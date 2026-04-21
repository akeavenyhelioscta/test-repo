{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Power Model (monthly)
-- Grain: 1 row per month
-- Extracts 15 PJM columns from the wide-format US Regional Power Model
--
-- NOTE: Auto-generated column names derived from EA API metadata via
-- build_column_map(). Verify against actual database columns if errors occur.
---------------------------

WITH SOURCE AS (
    SELECT *
    FROM {{ ref('source_v1_ea_us_regional_power_model') }}
),

---------------------------
-- PJM column extraction
---------------------------

PJM AS (
    SELECT
        date

        -- Generation by fuel type (MW)
        ,fcst_coal_generation_under_ea_forecast_price_pjm_in_mw AS coal_generation_ea_price_mw
        ,fcst_coal_generation_under_forward_price_pjm_in_mw AS coal_generation_fwd_price_mw
        ,fcst_ng_generation_under_ea_price_pjm_in_mw AS ng_generation_ea_price_mw
        ,fcst_ng_generation_under_forward_price_pjm_in_mw AS ng_generation_fwd_price_mw
        ,fcst_nuclear_generation_pjm_in_mw AS nuclear_generation_mw
        ,fcst_solar_generation_pjm_in_mw AS solar_generation_mw
        ,fcst_wind_generation_pjm_in_mw AS wind_generation_mw
        ,fcst_hydro_generation_pjm_in_mw AS hydro_generation_mw
        ,fcst_other_generation_pjm_in_mw AS other_generation_mw
        ,fcst_thermal_generation_under_norm_weather_pjm_in_mw AS thermal_generation_norm_weather_mw
        ,fcst_net_imports_generation_pjm_in_mw AS net_imports_mw

        -- Demand (MW)
        ,ea_actual_load_and_forecast_load_unde_lances_pjm_in_mw_86d5c0ae AS demand_mw

        -- Natural gas demand (bcf/d)
        ,fcst_ng_demand_under_ea_price_pjm_in_bcf_per_d AS ng_demand_ea_price_bcf_per_d
        ,fcst_ng_demand_under_forward_price_pjm_in_bcf_per_d AS ng_demand_fwd_price_bcf_per_d
        ,fcst_ng_equivalent_demand_under_norm_weather_pjm_in_bcf_per_d AS ng_equiv_demand_norm_weather_bcf_per_d

    FROM SOURCE
),

FINAL AS (
    SELECT * FROM PJM
)

SELECT * FROM FINAL
ORDER BY date DESC
