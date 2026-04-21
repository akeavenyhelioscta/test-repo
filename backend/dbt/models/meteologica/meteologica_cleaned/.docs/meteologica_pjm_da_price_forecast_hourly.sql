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
  ,hub
  ,forecast_da_price
from meteologica_cleaned.meteologica_pjm_da_price_forecast_hourly
