{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Dispatch Costs (monthly)
-- Grain: 1 row per month
-- Extracts 36 PJM columns from ISO Dispatch Costs source
-- Covers NG, diesel, fuel oil, bituminous coal, sub-bituminous coal
-- by plant type (CCGT, CT, ST) and hub (PJM W, Dominion, Nihub, Adhub)
--
-- NOTE: Auto-generated column names derived from EA API metadata via
-- build_column_map(). Verify against actual database columns if errors occur.
---------------------------

WITH SOURCE AS (
    SELECT *
    FROM {{ ref('source_v1_ea_iso_dispatch_costs') }}
),

---------------------------
-- PJM column extraction
---------------------------

PJM AS (
    SELECT
        date

        -- PJM W: NG dispatch costs ($/MWh, RGGI included)
        ,fcst_ng_dis_cost_high_hr_ccgt_rggi_inc_in_pjm_w_in_usd_mwh AS ng_high_hr_ccgt_dispatch_cost_pjm_w
        ,fcst_ng_dis_cost_low_hr_ccgt_rggi_inc_in_pjm_w_in_usd_mwh AS ng_low_hr_ccgt_dispatch_cost_pjm_w
        ,fcst_ng_dis_cost_ct_rggi_inc_in_pjm_w_in_usd_mwh AS ng_ct_dispatch_cost_pjm_w
        ,fcst_ng_dis_cost_st_rggi_inc_in_pjm_w_in_usd_mwh AS ng_st_dispatch_cost_pjm_w

        -- PJM W: diesel and fuel oil dispatch costs ($/MWh, RGGI included)
        ,fcst_diesel_dis_cost_ccgt_rggi_inc_in_pjm_w_in_usd_mwh AS diesel_ccgt_dispatch_cost_pjm_w
        ,fcst_diesel_dis_cost_ct_rggi_inc_in_pjm_w_in_usd_mwh AS diesel_ct_dispatch_cost_pjm_w
        ,fcst_fuel_oil_dis_cost_st_rggi_inc_in_pjm_w_in_usd_mwh AS fuel_oil_st_dispatch_cost_pjm_w

        -- PJM Nihub: NG fuel costs ($/MWh)
        ,fcst_ng_fuel_cost_high_hr_ccgt_in_pjm_nihub_in_usd_mwh AS ng_high_hr_ccgt_fuel_cost_nihub
        ,fcst_ng_fuel_cost_low_hr_ccgt_in_pjm_nihub_in_usd_mwh AS ng_low_hr_ccgt_fuel_cost_nihub
        ,fcst_ng_fuel_cost_ct_in_pjm_nihub_in_usd_mwh AS ng_ct_fuel_cost_nihub
        ,fcst_ng_fuel_cost_st_in_pjm_nihub_in_usd_mwh AS ng_st_fuel_cost_nihub

        -- PJM Adhub: NG fuel costs ($/MWh)
        ,fcst_ng_fuel_cost_high_hr_ccgt_in_pjm_adhub_in_usd_mwh AS ng_high_hr_ccgt_fuel_cost_adhub
        ,fcst_ng_fuel_cost_low_hr_ccgt_in_pjm_adhub_in_usd_mwh AS ng_low_hr_ccgt_fuel_cost_adhub
        ,fcst_ng_fuel_cost_ct_in_pjm_adhub_in_usd_mwh AS ng_ct_fuel_cost_adhub
        ,fcst_ng_fuel_cost_st_in_pjm_adhub_in_usd_mwh AS ng_st_fuel_cost_adhub

        -- PJM W: NG fuel costs ($/MWh)
        ,fcst_ng_fuel_cost_high_hr_ccgt_in_pjm_w_in_usd_mwh AS ng_high_hr_ccgt_fuel_cost_pjm_w
        ,fcst_ng_fuel_cost_low_hr_ccgt_in_pjm_w_in_usd_mwh AS ng_low_hr_ccgt_fuel_cost_pjm_w
        ,fcst_ng_fuel_cost_ct_in_pjm_w_in_usd_mwh AS ng_ct_fuel_cost_pjm_w
        ,fcst_ng_fuel_cost_st_in_pjm_w_in_usd_mwh AS ng_st_fuel_cost_pjm_w

        -- PJM Dominion: NG fuel costs ($/MWh)
        ,fcst_ng_fuel_cost_high_hr_ccgt_in_pjm_dominion_in_usd_mwh AS ng_high_hr_ccgt_fuel_cost_dominion
        ,fcst_ng_fuel_cost_low_hr_ccgt_in_pjm_dominion_in_usd_mwh AS ng_low_hr_ccgt_fuel_cost_dominion
        ,fcst_ng_fuel_cost_ct_in_pjm_dominion_in_usd_mwh AS ng_ct_fuel_cost_dominion
        ,fcst_ng_fuel_cost_st_in_pjm_dominion_in_usd_mwh AS ng_st_fuel_cost_dominion

        -- PJM Dominion: NG dispatch costs ($/MWh, RGGI included)
        ,fcst_ng_dis_cost_high_hr_ccgt_rggi_in_inion_in_usd_mwh_9b77e8dd AS ng_high_hr_ccgt_dispatch_cost_dominion
        ,fcst_ng_dis_cost_low_hr_ccgt_rggi_inc_inion_in_usd_mwh_4fe1596c AS ng_low_hr_ccgt_dispatch_cost_dominion
        ,fcst_ng_dis_cost_ct_rggi_inc_in_pjm_dominion_in_usd_mwh AS ng_ct_dispatch_cost_dominion
        ,fcst_ng_dis_cost_st_rggi_inc_in_pjm_dominion_in_usd_mwh AS ng_st_dispatch_cost_dominion

        -- PJM W: bituminous coal dispatch costs ($/MWh, RGGI included)
        ,fcst_bit_coal_dis_cost_with_high_tran_pjm_w_in_usd_mwh_fd09fcc9 AS bit_coal_high_transport_dispatch_cost_pjm_w
        ,fcst_bit_coal_dis_cost_coal_plant_wit_pjm_w_in_usd_mwh_5517aa53 AS bit_coal_low_transport_dispatch_cost_pjm_w
        ,fcst_bit_coal_dis_cost_coal_plant_wit_pjm_w_in_usd_mwh_1dab02e6 AS bit_coal_mid_transport_dispatch_cost_pjm_w

        -- PJM W: bituminous coal fuel costs ($/MWh)
        ,fcst_bit_coal_fuel_cost_with_high_tra_pjm_w_in_usd_mwh_0344330f AS bit_coal_high_transport_fuel_cost_pjm_w
        ,fcst_bit_coal_fuel_cost_coal_plant_wi_pjm_w_in_usd_mwh_11657bac AS bit_coal_low_transport_fuel_cost_pjm_w
        ,fcst_bit_coal_fuel_cost_coal_plant_wi_pjm_w_in_usd_mwh_535150ae AS bit_coal_mid_transport_fuel_cost_pjm_w

        -- PJM Nihub: sub-bituminous coal fuel costs ($/MWh)
        ,fcst_sub_bit_coal_fuel_cost_with_high_nihub_in_usd_mwh_6b5fb0d0 AS sub_bit_coal_high_transport_fuel_cost_nihub
        ,fcst_sub_bit_coal_fuel_cost_coal_plan_nihub_in_usd_mwh_0d76d8db AS sub_bit_coal_low_transport_fuel_cost_nihub
        ,fcst_sub_bit_coal_fuel_cost_coal_plan_nihub_in_usd_mwh_142bd002 AS sub_bit_coal_mid_transport_fuel_cost_nihub

    FROM SOURCE
),

FINAL AS (
    SELECT * FROM PJM
)

SELECT * FROM FINAL
ORDER BY date DESC
