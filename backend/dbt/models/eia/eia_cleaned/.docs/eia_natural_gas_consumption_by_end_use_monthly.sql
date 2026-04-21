select
  year
  ,month
  ,area_name_standardized
  ,consumption_unit
  ,lease_and_plant_fuel
  ,pipeline_and_distribution_use
  ,volumes_delivered_to_consumers
  ,residential
  ,commercial
  ,industrial
  ,vehicle_fuel
  ,electric_power
from eia_cleaned.eia_natural_gas_consumption_by_end_use_monthly
