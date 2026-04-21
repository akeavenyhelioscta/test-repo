select
  date
  ,hub
  ,period
  ,market
  ,lmp_total
  ,lmp_system_energy_price
  ,lmp_congestion_price
  ,lmp_marginal_loss_price
from pjm_cleaned.pjm_lmps_daily
