select
  forecast_execution_datetime_utc
  ,timezone
  ,forecast_execution_datetime_local
  ,forecast_rank
  ,forecast_execution_date
  ,forecast_datetime
  ,forecast_date
  ,hour_ending
  ,wind_forecast
from pjm_cleaned.pjm_gridstatus_wind_forecast_hourly
