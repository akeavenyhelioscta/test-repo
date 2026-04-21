select
  gas_day
  ,power_burn_variable
  ,model_type_based_on_noms
  ,max_model_type_based_on_noms
  ,conus
  ,east
  ,midwest
  ,mountain
  ,pacific
  ,south_central
from genscape_cleaned.genscape_daily_power_estimate
