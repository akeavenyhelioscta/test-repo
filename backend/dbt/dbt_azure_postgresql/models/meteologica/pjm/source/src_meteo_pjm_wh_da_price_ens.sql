{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica: usa_pjm_western_hub_da_power_price_forecast_ecmwf_ens_hourly
-- Thin passthrough of raw Meteologica source table (schema: meteologica).
-- Western Hub DA power price — ECMWF ensemble (51 members + avg/bottom/top).
-- Grain: 1 row per update_id x forecast_period_start
---------------------------

SELECT
    content_id
    ,update_id
    ,issue_date
    ,forecast_period_start
    ,forecast_period_end
    ,average_price
    ,bottom_price
    ,top_price
    ,ens_00_price ,ens_01_price ,ens_02_price ,ens_03_price ,ens_04_price
    ,ens_05_price ,ens_06_price ,ens_07_price ,ens_08_price ,ens_09_price
    ,ens_10_price ,ens_11_price ,ens_12_price ,ens_13_price ,ens_14_price
    ,ens_15_price ,ens_16_price ,ens_17_price ,ens_18_price ,ens_19_price
    ,ens_20_price ,ens_21_price ,ens_22_price ,ens_23_price ,ens_24_price
    ,ens_25_price ,ens_26_price ,ens_27_price ,ens_28_price ,ens_29_price
    ,ens_30_price ,ens_31_price ,ens_32_price ,ens_33_price ,ens_34_price
    ,ens_35_price ,ens_36_price ,ens_37_price ,ens_38_price ,ens_39_price
    ,ens_40_price ,ens_41_price ,ens_42_price ,ens_43_price ,ens_44_price
    ,ens_45_price ,ens_46_price ,ens_47_price ,ens_48_price ,ens_49_price
    ,ens_50_price
FROM {{ source('meteologica_pjm_v1', 'usa_pjm_western_hub_da_power_price_forecast_ecmwf_ens_hourly') }}
