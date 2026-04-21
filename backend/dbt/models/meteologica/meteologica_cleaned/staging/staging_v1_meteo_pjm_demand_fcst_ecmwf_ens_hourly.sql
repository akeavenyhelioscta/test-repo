{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM ECMWF-ENS Demand Forecast (Hourly)
-- UNIONs 36 raw tables (RTO + 3 macro regions + 32 sub-regions), normalizes to EPT date + hour_ending,
-- ranks by issue time (earliest first)
-- Grain: 1 row per forecast_rank x forecast_date x hour_ending x region
---------------------------

WITH UNIONED AS (

    ---------------------------
    -- RTO + 3 macro regions
    ---------------------------

    SELECT
        'RTO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'SOUTH' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_south_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    ---------------------------
    -- Mid-Atlantic sub-regions (17)
    ---------------------------

    SELECT
        'MIDATL_AE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_ae_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_BC' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_bc_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_DPL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_dpl_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_DPL_DPLCO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_dpl_dplco_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_DPL_EASTON' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_dpl_easton_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_JC' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_jc_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_ME' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_me_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pe_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PEP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pep_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PEP_PEPCO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pep_pepco_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PEP_SMECO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pep_smeco_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pl_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PL_PLCO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pl_plco_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PL_UGI' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pl_ugi_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PN' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pn_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_PS' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_ps_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'MIDATL_RECO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_reco_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    ---------------------------
    -- South sub-regions (1)
    ---------------------------

    SELECT
        'SOUTH_DOM' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_south_dom_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    ---------------------------
    -- West sub-regions (14)
    ---------------------------

    SELECT
        'WEST_AEP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPAPT' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepapt_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPIMP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepimp_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPKPT' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepkpt_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_AEP_AEPOPT' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_aep_aepopt_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_AP' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_ap_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_ATSI' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_atsi_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_ATSI_OE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_atsi_oe_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_ATSI_PAPWR' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_atsi_papwr_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_CE' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_ce_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_DAY' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_day_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_DEOK' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_deok_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_DUQ' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_duq_power_demand_forecast_ecmwf_ens_hourly') }}

    UNION ALL

    SELECT
        'WEST_EKPC' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,average_mw
        ,bottom_mw
        ,top_mw
        ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
        ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
        ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
        ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
        ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
        ,ens_50_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_ekpc_power_demand_forecast_ecmwf_ens_hourly') }}

),

---------------------------
-- NORMALIZE TIMESTAMPS (UTC + timezone + local triplets)
---------------------------

NORMALIZED AS (
    SELECT
        region
        ,issue_date::TIMESTAMP AS forecast_execution_datetime_utc
        ,'US/Eastern' AS timezone
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS forecast_execution_datetime_local
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE AS forecast_execution_date

        ,((forecast_period_start + INTERVAL '1 hour') AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS forecast_datetime_ending_utc
        ,(forecast_period_start + INTERVAL '1 hour') AS forecast_datetime_ending_local
        ,forecast_period_start::DATE AS forecast_date
        ,EXTRACT(HOUR FROM forecast_period_start)::INT + 1 AS hour_ending

        ,average_mw::NUMERIC AS average_mw
        ,bottom_mw::NUMERIC AS bottom_mw
        ,top_mw::NUMERIC AS top_mw
        ,ens_00_mw::NUMERIC AS ens_00_mw ,ens_01_mw::NUMERIC AS ens_01_mw ,ens_02_mw::NUMERIC AS ens_02_mw ,ens_03_mw::NUMERIC AS ens_03_mw ,ens_04_mw::NUMERIC AS ens_04_mw
        ,ens_05_mw::NUMERIC AS ens_05_mw ,ens_06_mw::NUMERIC AS ens_06_mw ,ens_07_mw::NUMERIC AS ens_07_mw ,ens_08_mw::NUMERIC AS ens_08_mw ,ens_09_mw::NUMERIC AS ens_09_mw
        ,ens_10_mw::NUMERIC AS ens_10_mw ,ens_11_mw::NUMERIC AS ens_11_mw ,ens_12_mw::NUMERIC AS ens_12_mw ,ens_13_mw::NUMERIC AS ens_13_mw ,ens_14_mw::NUMERIC AS ens_14_mw
        ,ens_15_mw::NUMERIC AS ens_15_mw ,ens_16_mw::NUMERIC AS ens_16_mw ,ens_17_mw::NUMERIC AS ens_17_mw ,ens_18_mw::NUMERIC AS ens_18_mw ,ens_19_mw::NUMERIC AS ens_19_mw
        ,ens_20_mw::NUMERIC AS ens_20_mw ,ens_21_mw::NUMERIC AS ens_21_mw ,ens_22_mw::NUMERIC AS ens_22_mw ,ens_23_mw::NUMERIC AS ens_23_mw ,ens_24_mw::NUMERIC AS ens_24_mw
        ,ens_25_mw::NUMERIC AS ens_25_mw ,ens_26_mw::NUMERIC AS ens_26_mw ,ens_27_mw::NUMERIC AS ens_27_mw ,ens_28_mw::NUMERIC AS ens_28_mw ,ens_29_mw::NUMERIC AS ens_29_mw
        ,ens_30_mw::NUMERIC AS ens_30_mw ,ens_31_mw::NUMERIC AS ens_31_mw ,ens_32_mw::NUMERIC AS ens_32_mw ,ens_33_mw::NUMERIC AS ens_33_mw ,ens_34_mw::NUMERIC AS ens_34_mw
        ,ens_35_mw::NUMERIC AS ens_35_mw ,ens_36_mw::NUMERIC AS ens_36_mw ,ens_37_mw::NUMERIC AS ens_37_mw ,ens_38_mw::NUMERIC AS ens_38_mw ,ens_39_mw::NUMERIC AS ens_39_mw
        ,ens_40_mw::NUMERIC AS ens_40_mw ,ens_41_mw::NUMERIC AS ens_41_mw ,ens_42_mw::NUMERIC AS ens_42_mw ,ens_43_mw::NUMERIC AS ens_43_mw ,ens_44_mw::NUMERIC AS ens_44_mw
        ,ens_45_mw::NUMERIC AS ens_45_mw ,ens_46_mw::NUMERIC AS ens_46_mw ,ens_47_mw::NUMERIC AS ens_47_mw ,ens_48_mw::NUMERIC AS ens_48_mw ,ens_49_mw::NUMERIC AS ens_49_mw
        ,ens_50_mw::NUMERIC AS ens_50_mw
    FROM UNIONED
),

--------------------------------
-- Rank forecasts per (forecast_date, region) by issue time (earliest first)
-- Partitioned by region because Meteologica regions come from
-- separate API endpoints with potentially different issue_dates
--------------------------------

FORECAST_RANK AS (
    SELECT
        forecast_date
        ,region
        ,forecast_execution_datetime_local

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date, region
            ORDER BY forecast_execution_datetime_local ASC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_datetime_local, forecast_date, region
        FROM NORMALIZED
    ) sub
),

--------------------------------
--------------------------------

FINAL AS (
    SELECT
        r.forecast_rank

        ,n.forecast_execution_datetime_utc
        ,n.timezone
        ,n.forecast_execution_datetime_local
        ,n.forecast_execution_date

        ,n.forecast_datetime_ending_utc
        ,n.forecast_datetime_ending_local
        ,n.forecast_date
        ,n.hour_ending

        ,n.region
        ,n.average_mw AS forecast_load_average_mw
        ,n.bottom_mw AS forecast_load_bottom_mw
        ,n.top_mw AS forecast_load_top_mw
        ,n.ens_00_mw ,n.ens_01_mw ,n.ens_02_mw ,n.ens_03_mw ,n.ens_04_mw ,n.ens_05_mw ,n.ens_06_mw ,n.ens_07_mw ,n.ens_08_mw ,n.ens_09_mw
        ,n.ens_10_mw ,n.ens_11_mw ,n.ens_12_mw ,n.ens_13_mw ,n.ens_14_mw ,n.ens_15_mw ,n.ens_16_mw ,n.ens_17_mw ,n.ens_18_mw ,n.ens_19_mw
        ,n.ens_20_mw ,n.ens_21_mw ,n.ens_22_mw ,n.ens_23_mw ,n.ens_24_mw ,n.ens_25_mw ,n.ens_26_mw ,n.ens_27_mw ,n.ens_28_mw ,n.ens_29_mw
        ,n.ens_30_mw ,n.ens_31_mw ,n.ens_32_mw ,n.ens_33_mw ,n.ens_34_mw ,n.ens_35_mw ,n.ens_36_mw ,n.ens_37_mw ,n.ens_38_mw ,n.ens_39_mw
        ,n.ens_40_mw ,n.ens_41_mw ,n.ens_42_mw ,n.ens_43_mw ,n.ens_44_mw ,n.ens_45_mw ,n.ens_46_mw ,n.ens_47_mw ,n.ens_48_mw ,n.ens_49_mw
        ,n.ens_50_mw

    FROM NORMALIZED n
    JOIN FORECAST_RANK r
        ON n.forecast_execution_datetime_local = r.forecast_execution_datetime_local
        AND n.forecast_date = r.forecast_date
        AND n.region = r.region
)

SELECT * FROM FINAL
ORDER BY forecast_date DESC, forecast_execution_datetime_local DESC, hour_ending, region


