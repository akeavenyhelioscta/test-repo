{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-- Staging: Genscape Daily Pipeline Production
-- Adds revision tracking to the cleaned source data.
-- Grain: 1 row per date x report_date
-------------------------------------------------------------

WITH SOURCE AS (
    SELECT * FROM {{ ref('source_v2_daily_pipeline_production') }}
),

-------------------------------------------------------------
-------------------------------------------------------------

REVISIONS AS (
    SELECT
        *
        ,ROW_NUMBER() OVER (PARTITION BY date ORDER BY report_date) as revision
    FROM SOURCE
),

-------------------------------------------------------------
-------------------------------------------------------------

MAX_REVISIONS AS (
    SELECT
        *
        ,MAX(revision) OVER (PARTITION BY date) AS max_revision
    FROM REVISIONS
),

-------------------------------------------------------------
-------------------------------------------------------------

FINAL AS (
    SELECT

        date

        -- revision
        ,report_date
        ,revision
        ,max_revision

        -- lower_48
        ,lower_48

        -- gulf_of_mexico
        ,gulf_of_mexico

        -- gulf_coast
        ,gulf_coast
        ,north_louisiana
        ,south_louisiana
        ,other_gulf_coast

        -- texas
        ,texas
        ,east_texas
        ,south_texas

        -- mid_con
        ,mid_con
        ,oklahoma
        ,kansas
        ,arkansas

        -- permian
        ,permian
        ,permian_new_mexico
        ,permian_texas

        -- permian_flare
        ,permian_flare_counts
        ,permian_flare_volume

        -- san_juan
        ,san_juan

        -- rockies
        ,rockies
        ,piceance_basin
        ,colorado_denver_julesberg
        ,north_dakota_montana
        ,utah_uinta
        ,wyoming_green_wind_ot
        ,wyoming_powder
        ,other_rockies

        -- west
        ,west

        -- east
        ,east
        ,ohio
        ,southwest_pennsylvania
        ,northeast_pennsylvania
        ,west_virginia
        ,other_east_ga_il_in_md_nc_tn_ky_mi_ny_va

        -- western_canada
        ,western_canada

    FROM MAX_REVISIONS
)

SELECT * FROM FINAL
ORDER BY date DESC, report_date DESC