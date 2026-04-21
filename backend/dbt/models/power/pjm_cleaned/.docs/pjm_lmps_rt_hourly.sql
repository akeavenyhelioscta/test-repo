select
  datetime
  ,date
  ,hour_ending
  ,hub
  ,rt_lmp_total
  ,rt_lmp_system_energy_price
  ,rt_lmp_congestion_price
  ,rt_lmp_marginal_loss_price
from pjm_cleaned.pjm_lmps_rt_hourly
