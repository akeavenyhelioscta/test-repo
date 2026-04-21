{% docs ercot_gridstatus_source %}

ERCOT data ingested via **GridStatus** -- both the open-source Python library and the
paid GridStatus.io API.

Tables in this source are stored in the `gridstatus` schema. Data is pulled by
scheduled Python scripts in `backend/scrapes/power/gridstatus_open_source/ercot/` and
`backend/scrapes/power/gridstatusio_api_key/ercot/`.

**Included datasets:**
- Day-ahead LMPs by settlement point (`ercot_lmp_by_settlement_point`)
- Real-time 15-minute SPPs (`ercot_spp_real_time_15_min`)
- 5-minute fuel mix by fuel type (`ercot_fuel_mix`)
- 5-minute energy storage resources (`ercot_energy_storage_resources`)
- Hourly load by forecast zone (`ercot_load_by_forecast_zone`)
- 7-day load forecast by forecast zone (`ercot_load_forecast_by_forecast_zone`)
- Hourly solar actual and forecast by geographic region (`ercot_solar_actual_and_forecast_by_geo_region_hourly`)
- Hourly wind actual and forecast by geographic region (`ercot_wind_actual_and_forecast_by_geo_region_hourly`)
- Reported outages (`ercot_reported_outages`)

{% enddocs %}
