-- ercot_forecasts_daily
-- Grain: 1 row per rank_forecast_execution_timestamps x forecast_date x period
-- Schema: ercot_cleaned

SELECT
    rank_forecast_execution_timestamps,  -- INT        Forecast recency rank (1 = latest)
    labelled_forecast_execution_timestamp,
    forecast_execution_datetime,
    forecast_execution_date,
    forecast_date,                       -- DATE       Target operating date
    period,                              -- TEXT       flat / onpeak / offpeak

    -- Forecast averages (avg MW)
    forecast_load_total,
    forecast_net_load_total,
    forecast_solar_total,
    forecast_wind_total

FROM ercot_cleaned.ercot_forecasts_daily
