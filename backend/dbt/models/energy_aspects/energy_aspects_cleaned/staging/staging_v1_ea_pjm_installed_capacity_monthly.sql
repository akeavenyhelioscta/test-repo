{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Installed Capacity by Fuel Type (monthly)
-- Grain: 1 row per month
-- Extracts 9 PJM columns from US Installed Capacity source
--
-- NOTE: Auto-generated column names derived from EA API metadata via
-- build_column_map(). Verify against actual database columns if errors occur.
---------------------------

WITH SOURCE AS (
    SELECT *
    FROM {{ ref('source_v1_ea_us_installed_cap_by_iso_fuel') }}
),

---------------------------
-- PJM column extraction
---------------------------

PJM AS (
    SELECT
        date
        ,fcst_ng_installed_capacity_in_pjm_in_mw AS ng_capacity_mw
        ,fcst_coal_installed_capacity_in_pjm_in_mw AS coal_capacity_mw
        ,fcst_nuclear_installed_capacity_in_pjm_in_mw AS nuclear_capacity_mw
        ,fcst_oil_products_installed_capacity_in_pjm_in_mw AS oil_capacity_mw
        ,fcst_solar_installed_capacity_in_pjm_in_mw AS solar_capacity_mw
        ,fcst_onshore_wind_installed_capacity_in_pjm_in_mw AS onshore_wind_capacity_mw
        ,fcst_offshore_wind_installed_capacity_in_pjm_in_mw AS offshore_wind_capacity_mw
        ,fcst_hydro_installed_capacity_in_pjm_in_mw AS hydro_capacity_mw
        ,fcst_battery_installed_capacity_in_pjm_in_mw AS battery_capacity_mw
    FROM SOURCE
),

FINAL AS (
    SELECT * FROM PJM
)

SELECT * FROM FINAL
ORDER BY date DESC
