select
  datetime
  ,date
  ,hour_ending
  ,hub
  ,market
  ,lmp_total
  ,lmp_system_energy_price
  ,lmp_congestion_price
  ,lmp_marginal_loss_price
from caiso_cleaned.caiso_lmps_hourly
