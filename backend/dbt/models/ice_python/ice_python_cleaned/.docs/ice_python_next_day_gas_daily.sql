select
  gas_day
  ,trade_date
  ,AVG(CASE WHEN hour_ending = 10 THEN hh_cash END) as hh_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN transco_st85_cash END) as transco_st85_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN pine_prarie_cash END) as pine_prarie_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN waha_cash END) as waha_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN houston_ship_channel_cash END) as houston_ship_channel_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN ngpl_txok_cash END) as ngpl_txok_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN transco_zone_5_south_cash END) as transco_zone_5_south_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN tetco_m3_cash END) as tetco_m3_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN agt_cash END) as agt_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN iroquois_z2_cash END) as iroquois_z2_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN socal_cg_cash END) as socal_cg_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN pge_cg_cash END) as pge_cg_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN cig_cash END) as cig_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN ngpl_midcon_cash END) as ngpl_midcon_cash
  ,AVG(CASE WHEN hour_ending = 10 THEN michcon_cash END) as michcon_cash
from ice_python_cleaned.ice_python_next_day_gas_hourly
group by gas_day, trade_date
order by gas_day desc
