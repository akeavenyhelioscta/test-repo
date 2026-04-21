{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RAW EIA-930 FUEL TYPE HOURLY GENERATION
---------------------------

WITH RAW AS (
    SELECT
        datetime_utc::TIMESTAMP AS datetime_utc,
        date::DATE AS date,
        hour::INTEGER AS hour,
        respondent::VARCHAR AS respondent,
        battery_storage::NUMERIC AS battery_storage,
        coal::NUMERIC AS coal,
        geothermal::NUMERIC AS geothermal,
        hydro::NUMERIC AS hydro,
        natural_gas::NUMERIC AS natural_gas,
        nuclear::NUMERIC AS nuclear,
        other::NUMERIC AS other,
        other_energy_storage::NUMERIC AS other_energy_storage,
        petroleum::NUMERIC AS petroleum,
        pumped_storage::NUMERIC AS pumped_storage,
        solar::NUMERIC AS solar,
        solar_with_integrated_battery_storage::NUMERIC AS solar_with_integrated_battery_storage,
        unknown::NUMERIC AS unknown,
        unknown_energy_storage::NUMERIC AS unknown_energy_storage,
        wind::NUMERIC AS wind,
        wind_with_integrated_battery_storage::NUMERIC AS wind_with_integrated_battery_storage
    FROM {{ source('eia_v1', 'fuel_type_hrl_gen_v3_2026_mar_09') }}
),

FINAL AS (
    SELECT * FROM RAW
)

SELECT * FROM FINAL
