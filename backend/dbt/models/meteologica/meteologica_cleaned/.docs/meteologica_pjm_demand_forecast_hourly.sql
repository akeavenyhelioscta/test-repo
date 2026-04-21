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
  ,forecast_load_mw
from meteologica_cleaned.meteologica_pjm_demand_forecast_hourly
