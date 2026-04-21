select
  forecast_rank
  ,forecast_execution_datetime_utc
  ,timezone
  ,forecast_execution_datetime_local
  ,forecast_execution_date
  ,forecast_datetime_ending_utc
  ,forecast_datetime_ending_local
  ,forecast_date
  ,hour_ending
  ,region
  ,forecast_load_average_mw
  ,forecast_load_bottom_mw
  ,forecast_load_top_mw
  ,ens_00_mw ,ens_01_mw ,ens_02_mw ,ens_03_mw ,ens_04_mw ,ens_05_mw ,ens_06_mw ,ens_07_mw ,ens_08_mw ,ens_09_mw
  ,ens_10_mw ,ens_11_mw ,ens_12_mw ,ens_13_mw ,ens_14_mw ,ens_15_mw ,ens_16_mw ,ens_17_mw ,ens_18_mw ,ens_19_mw
  ,ens_20_mw ,ens_21_mw ,ens_22_mw ,ens_23_mw ,ens_24_mw ,ens_25_mw ,ens_26_mw ,ens_27_mw ,ens_28_mw ,ens_29_mw
  ,ens_30_mw ,ens_31_mw ,ens_32_mw ,ens_33_mw ,ens_34_mw ,ens_35_mw ,ens_36_mw ,ens_37_mw ,ens_38_mw ,ens_39_mw
  ,ens_40_mw ,ens_41_mw ,ens_42_mw ,ens_43_mw ,ens_44_mw ,ens_45_mw ,ens_46_mw ,ens_47_mw ,ens_48_mw ,ens_49_mw
  ,ens_50_mw
from meteologica_cleaned.meteologica_pjm_demand_forecast_ecmwf_ens_hourly
