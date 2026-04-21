select
  forecast_rank
  ,forecast_execution_date
  ,forecast_date
  ,forecast_day_number
  ,region
  ,total_outages_mw
  ,planned_outages_mw
  ,maintenance_outages_mw
  ,forced_outages_mw
from pjm_cleaned.pjm_outages_forecast_daily
