{{
  config(
    materialized='view'
  )
}}

----------------------------------------------------
-- MODEL SCORECARD — the LATEST published forecast for each delivery date,
-- per model, carried alongside the full scorecard_da_onpeak_wh_ice row
-- (DA HE 8-23 component breakdown + ICE strip OHLC/vwap/volume + ice_error)
-- and scored against both the realized clear and the curve.
--
-- Grain: one row per (model_name, delivery_date) — latest vintage only
-- (model_forecasts_revisions.forecast_run_revision = 1, i.e. the run with
-- the largest run_date — the model's standing call for that delivery). For
-- the full vintage-by-vintage revision trail use model_forecasts_revisions;
-- for the ICE-priced delivery spine with every model fanned out on the right
-- (incl. deliveries no model has touched, plus the run payload jsonb) use
-- scorecard_model_onpeak_wh.
--
-- Columns 1..20 are scorecard_da_onpeak_wh_ice verbatim (LEFT JOIN on
-- delivery_date = target_date — already 2dp). Forward-dated deliveries the
-- model has called but ICE has not yet priced carry NULL DA / ICE columns.
-- Then the model columns:
--   da_lmp_total_forecast — the model's standing OnPeak HE 8-23 point call.
--   model_vs_actual = da_lmp_total_forecast - da_lmp_total  (+ = model too high)
--   model_vs_ice    = da_lmp_total_forecast - ice_vwap      (model's edge vs the curve)
-- Both diffs are NULL until their right-hand side exists.
-- Ordered model_name, then delivery_date DESC (most recent delivery on top).
----------------------------------------------------

WITH latest_forecast AS (
    SELECT
        model_name
        ,model_family
        ,target_date
        ,run_date
        ,da_lmp_total_onpeak_forecast AS da_lmp_total_forecast
        ,payload
    FROM {{ ref('model_forecasts_revisions') }}
    WHERE forecast_run_revision = 1
)

SELECT
    f.target_date AS delivery_date

    ,ice.da_lmp_total
    ,ice.da_lmp_system_energy
    ,ice.da_lmp_congestion
    ,ice.da_lmp_loss

    ,ice.ice_start_date
    ,ice.ice_end_date
    ,ice.product
    ,ice.ice_description
    ,ice.ice_trade_date_start
    ,ice.ice_trade_date_last

    ,ice.ice_open
    ,ice.ice_high
    ,ice.ice_low
    ,ice.ice_close
    ,ice.ice_vwap
    ,ice.ice_volume
    ,ice.ice_buy_volume
    ,ice.ice_sell_volume

    ,ice.actual_vs_ice

    ,f.model_name
    ,f.model_family
    ,f.target_date
    ,f.run_date
    ,ROUND(f.da_lmp_total_forecast::NUMERIC,                      2) AS da_lmp_total_forecast
    ,f.payload
    ,ROUND((f.da_lmp_total_forecast - ice.da_lmp_total)::NUMERIC, 2) AS model_vs_actual
    ,ROUND((f.da_lmp_total_forecast - ice.ice_vwap)::NUMERIC,     2) AS model_vs_ice
FROM latest_forecast f
LEFT JOIN {{ ref('scorecard_da_onpeak_wh_ice') }} ice
    ON ice.delivery_date = f.target_date
ORDER BY f.model_name, f.target_date DESC
