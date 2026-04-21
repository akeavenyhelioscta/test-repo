-- ercot_forecasts_hourly
-- Grain: 1 row per rank_forecast_execution_timestamps x forecast_date x hour_ending
-- Schema: ercot_cleaned

SELECT
    rank_forecast_execution_timestamps,  -- INT        Forecast recency rank (1 = latest)
    labelled_forecast_execution_timestamp,
    forecast_execution_datetime,
    forecast_execution_date,
    interval_start,
    forecast_date,                       -- DATE       Target operating date
    hour_ending,                         -- INT        Hour ending (1-24)

    -- Load forecast by zone (MW)
    forecast_load_total,
    forecast_load_north,
    forecast_load_south,
    forecast_load_west,
    forecast_load_houston,

    -- Renewable forecasts (MW)
    forecast_solar_total,
    forecast_wind_total,

    -- Net load (MW): load - solar - wind
    forecast_net_load_total

FROM ercot_cleaned.ercot_forecasts_hourly
