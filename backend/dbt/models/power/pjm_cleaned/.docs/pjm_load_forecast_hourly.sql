select
  forecast_execution_datetime_utc
  ,timezone
  ,forecast_execution_datetime_local
  ,forecast_rank
  ,forecast_execution_date
  ,forecast_datetime
  ,forecast_date
  ,hour_ending
  ,region
  ,forecast_load_mw
from pjm_cleaned.pjm_load_forecast_hourly
