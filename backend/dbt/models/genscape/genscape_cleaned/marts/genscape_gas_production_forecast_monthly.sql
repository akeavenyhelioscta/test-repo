{{
  config(
    materialized='view'
  )
}}

-------------------------------------------------------------
-- Monthly Gas Production Forecast
-- Filters to only official published report dates using the
-- seed: genscape_gas_production_forecast_report_dates.
-- Revision numbers are recalculated scoped to monthly only.
-------------------------------------------------------------

WITH monthly_report_dates AS (
    SELECT report_date::DATE as report_date
    FROM {{ ref('genscape_gas_production_forecast_report_dates') }}
),

staging AS (
    SELECT * FROM {{ ref('staging_v2_genscape_gas_production_forecast') }}
),

filtered AS (
    SELECT s.*
    FROM staging s
    INNER JOIN monthly_report_dates m
        ON s.report_date = m.report_date
),

recalc_revisions AS (
    SELECT
        year
        ,month
        ,date

        ,report_date
        ,ROW_NUMBER() OVER (PARTITION BY date ORDER BY report_date) as revision
        ,COUNT(*) OVER (PARTITION BY date) as max_revision

        ,lower_48_production
        ,lower_48_dry_gas_production_yoy
        ,lower_48_oil_rig_count
        ,lower_48_gas_rig_count

        ,gulf_of_mexico_production
        ,gulf_of_mexico_dry_gas_production_yoy
        ,gulf_of_mexico_oil_rig_count
        ,gulf_of_mexico_gas_rig_count

        ,gulf_coast_production
        ,gulf_coast_dry_gas_production_yoy
        ,gulf_coast_oil_rig_count
        ,gulf_coast_gas_rig_count
        ,north_louisiana_production
        ,north_louisiana_dry_gas_production_yoy
        ,north_louisiana_oil_rig_count
        ,north_louisiana_gas_rig_count
        ,south_louisiana_production
        ,south_louisiana_dry_gas_production_yoy
        ,south_louisiana_oil_rig_count
        ,south_louisiana_gas_rig_count

        ,texas_production
        ,texas_dry_gas_production_yoy
        ,texas_oil_rig_count
        ,texas_gas_rig_count
        ,south_texas_production
        ,south_texas_dry_gas_production_yoy
        ,south_texas_oil_rig_count
        ,south_texas_gas_rig_count
        ,east_texas_production
        ,east_texas_dry_gas_production_yoy
        ,east_texas_oil_rig_count
        ,east_texas_gas_rig_count

        ,mid_con_production
        ,mid_con_dry_gas_production_yoy
        ,mid_con_oil_rig_count
        ,mid_con_gas_rig_count

        ,permian_production
        ,permian_dry_gas_production_yoy
        ,permian_oil_rig_count
        ,permian_gas_rig_count

        ,san_juan_production
        ,san_juan_dry_gas_production_yoy
        ,san_juan_oil_rig_count
        ,san_juan_gas_rig_count

        ,rockies_production
        ,rockies_dry_gas_production_yoy
        ,rockies_oil_rig_count
        ,rockies_gas_rig_count

        ,west_production
        ,west_dry_gas_production_yoy
        ,west_oil_rig_count
        ,west_gas_rig_count

        ,east_production
        ,east_dry_gas_production_yoy
        ,east_oil_rig_count
        ,east_gas_rig_count

        ,western_canada_production
        ,western_canada_dry_gas_production_yoy
        ,western_canada_oil_rig_count
        ,western_canada_gas_rig_count

        ,ohio_production
        ,ohio_dry_gas_production_yoy
        ,ohio_oil_rig_count
        ,ohio_gas_rig_count
        ,pennsylvania_production
        ,pennsylvania_dry_gas_production_yoy
        ,pennsylvania_oil_rig_count
        ,pennsylvania_gas_rig_count
        ,ne_pennsylvania_production
        ,ne_pennsylvania_dry_gas_production_yoy
        ,ne_pennsylvania_oil_rig_count
        ,ne_pennsylvania_gas_rig_count
        ,sw_pennsylvania_production
        ,sw_pennsylvania_dry_gas_production_yoy
        ,sw_pennsylvania_oil_rig_count
        ,sw_pennsylvania_gas_rig_count
        ,virginia_production
        ,virginia_dry_gas_production_yoy
        ,virginia_oil_rig_count
        ,virginia_gas_rig_count
        ,west_virginia_production
        ,west_virginia_dry_gas_production_yoy
        ,west_virginia_oil_rig_count
        ,west_virginia_gas_rig_count
        ,other_east_production
        ,other_east_dry_gas_production_yoy
        ,other_east_oil_rig_count
        ,other_east_gas_rig_count

        ,alaska_production
        ,alaska_dry_gas_production_yoy
        ,alaska_oil_rig_count
        ,alaska_gas_rig_count

        ,nova_scotia_production
        ,nova_scotia_dry_gas_production_yoy
        ,nova_scotia_oil_rig_count
        ,nova_scotia_gas_rig_count

        ,united_states_production
        ,united_states_dry_gas_production_yoy
        ,united_states_oil_rig_count
        ,united_states_gas_rig_count

    FROM filtered
)

SELECT * FROM recalc_revisions
ORDER BY date ASC, report_date DESC