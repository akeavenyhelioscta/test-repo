{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-- Source: Genscape Daily Pipeline Production
-- Cleans raw columns, casts types, and computes composite regions.
-- Grain: 1 row per date x report_date
-------------------------------------------------------------

WITH SOURCE AS (
    SELECT

        date::DATE as date
        ,reportdate::DATE as report_date

        -- lower_48
        ,lower_48::NUMERIC as lower_48

        -- gulf_of_mexico
        ,gulf_of_mexico::NUMERIC as gulf_of_mexico

        -- gulf_coast (composite)
        ,(COALESCE(north_louisiana, 0) + COALESCE(south_louisiana, 0) + COALESCE(other_gulf_coast, 0))::NUMERIC as gulf_coast
        ,north_louisiana::NUMERIC as north_louisiana
        ,south_louisiana::NUMERIC as south_louisiana
        ,other_gulf_coast::NUMERIC as other_gulf_coast

        -- texas
        ,texas::NUMERIC as texas
        ,east_texas::NUMERIC as east_texas
        ,south_texas::NUMERIC as south_texas

        -- mid_con (composite)
        ,(COALESCE(oklahoma, 0) + COALESCE(kansas, 0) + COALESCE(arkansas, 0))::NUMERIC as mid_con
        ,oklahoma::NUMERIC as oklahoma
        ,kansas::NUMERIC as kansas
        ,arkansas::NUMERIC as arkansas

        -- permian (composite)
        ,(COALESCE(permian_new_mexico, 0) + COALESCE(permian_texas, 0))::NUMERIC as permian
        ,permian_new_mexico::NUMERIC as permian_new_mexico
        ,permian_texas::NUMERIC as permian_texas

        -- permian_flare
        ,permian_flare_counts::NUMERIC as permian_flare_counts
        ,permian_flare_volume::NUMERIC as permian_flare_volume

        -- san_juan
        ,san_juan::NUMERIC as san_juan

        -- rockies (composite)
        ,(COALESCE(piceance_basin, 0) + COALESCE(colorado_denver_julesberg, 0) + COALESCE(north_dakota_montana, 0) + COALESCE(utah_uinta, 0) + COALESCE(wyoming_green_wind_ot, 0) + COALESCE(wyoming_powder, 0) + COALESCE(other_rockies, 0))::NUMERIC as rockies
        ,piceance_basin::NUMERIC as piceance_basin
        ,colorado_denver_julesberg::NUMERIC as colorado_denver_julesberg
        ,north_dakota_montana::NUMERIC as north_dakota_montana
        ,utah_uinta::NUMERIC as utah_uinta
        ,wyoming_green_wind_ot::NUMERIC as wyoming_green_wind_ot
        ,wyoming_powder::NUMERIC as wyoming_powder
        ,other_rockies::NUMERIC as other_rockies

        -- west
        ,west::NUMERIC as west

        -- east (composite)
        ,(COALESCE(ohio, 0) + COALESCE(southwest_pennsylvania, 0) + COALESCE(northeast_pennsylvania, 0) + COALESCE(west_virginia, 0) + COALESCE(other_east_ga_il_in_md_nc_tn_ky_mi_ny_va, 0))::NUMERIC as east
        ,ohio::NUMERIC as ohio
        ,southwest_pennsylvania::NUMERIC as southwest_pennsylvania
        ,northeast_pennsylvania::NUMERIC as northeast_pennsylvania
        ,west_virginia::NUMERIC as west_virginia
        ,other_east_ga_il_in_md_nc_tn_ky_mi_ny_va::NUMERIC as other_east_ga_il_in_md_nc_tn_ky_mi_ny_va

        -- western_canada
        ,western_canada::NUMERIC as western_canada

    FROM {{ source('genscape_v2', 'daily_pipeline_production_v2_2026_mar_10') }}
),

FINAL AS (
    SELECT * FROM SOURCE
)

SELECT * FROM FINAL