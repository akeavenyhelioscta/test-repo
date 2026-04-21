{{
  config(
    materialized='incremental',
    unique_key=['date', 'respondent'],
    incremental_strategy='delete+insert'
  )
}}

---------------------------
-- EIA-930 DAILY GENERATION BY RESPONDENT
---------------------------

WITH DAILY AS (
    SELECT
        date,

        is_iso,
        time_zone,
        respondent,
        region,
        balancing_authority_name,

        -- load
        AVG(total) AS total_mw,
        AVG(renewables) AS renewables_mw,
        AVG(thermal) AS thermal_mw,

        -- renewables
        AVG(wind) AS wind_mw,
        AVG(solar) AS solar_mw,

        -- thermal
        AVG(natural_gas) AS natural_gas_mw,
        AVG(coal) AS coal_mw,
        AVG(oil) AS oil_mw,

        -- nuclear
        AVG(nuclear) AS nuclear_mw,

        -- hydro
        AVG(hydro) AS hydro_mw,
        AVG(pumped_storage) AS pumped_storage_mw,

        -- geothermal
        AVG(geothermal) AS geothermal_mw,

        -- battery
        AVG(battery) AS battery_mw,
        AVG(solar_battery) AS solar_battery_mw,
        AVG(wind_battery) AS wind_battery_mw,
        AVG(other_energy_storage) AS other_energy_storage_mw,
        AVG(unknown_energy_storage) AS unknown_energy_storage_mw,

        -- other
        AVG(other) AS other_mw,
        AVG(unknown) AS unknown_mw

    FROM {{ ref('staging_v1_eia_930_hourly') }}
    GROUP BY date, is_iso, time_zone, respondent, region, balancing_authority_name
),

---------------------------
-- ADD THERMAL PERCENTAGES
---------------------------

FINAL AS (
    SELECT
        date,

        is_iso,
        time_zone,
        respondent,
        region,
        balancing_authority_name,

        -- load
        total_mw,
        renewables_mw,
        thermal_mw,

        -- renewables
        wind_mw,
        solar_mw,

        -- thermal
        natural_gas_mw,
        coal_mw,
        oil_mw,

        -- nuclear
        nuclear_mw,

        -- hydro
        hydro_mw,
        pumped_storage_mw,

        -- geothermal
        geothermal_mw,

        -- battery
        battery_mw,
        solar_battery_mw,
        wind_battery_mw,
        other_energy_storage_mw,
        unknown_energy_storage_mw,

        -- other
        other_mw,
        unknown_mw,

        -- pct of thermal
        natural_gas_mw / NULLIF(thermal_mw, 0) AS natural_gas_pct_of_thermal,
        coal_mw / NULLIF(thermal_mw, 0) AS coal_pct_of_thermal

    FROM DAILY
)

SELECT * FROM FINAL

{% if is_incremental() %}
WHERE date >= (SELECT MAX(date) - INTERVAL '10 days' FROM {{ this }})
{% endif %}
