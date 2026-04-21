select
  datetime_beginning_utc
  ,datetime_ending_utc
  ,timezone
  ,datetime_beginning_local
  ,datetime_ending_local
  ,date
  ,hour_ending
  ,tie_flow_name
  ,actual_mw
  ,scheduled_mw
from pjm_cleaned.pjm_tie_flows_hourly
