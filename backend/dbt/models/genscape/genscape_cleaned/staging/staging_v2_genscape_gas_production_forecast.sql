{{
  config(
    materialized='table'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH GENSCAPE_PROD_FORECAST_REGIONS AS (
    SELECT  

        report_date

        ,year
        ,month
        ,date

        -- 'Lower 48'
        ,SUM(CASE WHEN region in ('Lower 48') THEN production END) as lower_48_production
        ,SUM(CASE WHEN region in ('Lower 48') THEN dry_gas_production_yoy END) as lower_48_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Lower 48') THEN oil_rig_count END) as lower_48_oil_rig_count
        ,SUM(CASE WHEN region in ('Lower 48') THEN gas_rig_count END) as lower_48_gas_rig_count

        -- 'Gulf of Mexico'
        ,SUM(CASE WHEN region in ('Gulf of Mexico - Deepwater', 'Gulf of Mexico - Shelf') THEN production END) as gulf_of_mexico_production
        ,SUM(CASE WHEN region in ('Gulf of Mexico - Deepwater', 'Gulf of Mexico - Shelf') THEN dry_gas_production_yoy END) as gulf_of_mexico_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Gulf of Mexico - Deepwater', 'Gulf of Mexico - Shelf') THEN oil_rig_count END) as gulf_of_mexico_oil_rig_count
        ,SUM(CASE WHEN region in ('Gulf of Mexico - Deepwater', 'Gulf of Mexico - Shelf') THEN gas_rig_count END) as gulf_of_mexico_gas_rig_count

        -- 'Gulf Coast'
        ,SUM(CASE WHEN region in ('Alabama', 'Florida', 'Mississippi', 'North Louisiana', 'South Louisiana') THEN production END) as gulf_coast_production
        ,SUM(CASE WHEN region in ('Alabama', 'Florida', 'Mississippi', 'North Louisiana', 'South Louisiana') THEN dry_gas_production_yoy END) as gulf_coast_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Alabama', 'Florida', 'Mississippi', 'North Louisiana', 'South Louisiana') THEN oil_rig_count END) as gulf_coast_oil_rig_count
        ,SUM(CASE WHEN region in ('Alabama', 'Florida', 'Mississippi', 'North Louisiana', 'South Louisiana') THEN gas_rig_count END) as gulf_coast_gas_rig_count
        -- 'North Louisiana'
        ,SUM(CASE WHEN region in ('North Louisiana') THEN production END) as north_louisiana_production
        ,SUM(CASE WHEN region in ('North Louisiana') THEN dry_gas_production_yoy END) as north_louisiana_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('North Louisiana') THEN oil_rig_count END) as north_louisiana_oil_rig_count
        ,SUM(CASE WHEN region in ('North Louisiana') THEN gas_rig_count END) as north_louisiana_gas_rig_count
        -- 'South Louisiana'
        ,SUM(CASE WHEN region in ('South Louisiana') THEN production END) as south_louisiana_production
        ,SUM(CASE WHEN region in ('South Louisiana') THEN dry_gas_production_yoy END) as south_louisiana_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('South Louisiana') THEN oil_rig_count END) as south_louisiana_oil_rig_count
        ,SUM(CASE WHEN region in ('South Louisiana') THEN gas_rig_count END) as south_louisiana_gas_rig_count

        -- 'South Texas'
        ,SUM(CASE WHEN region in ('Texas Dist 1', 'Texas Dist 2', 'Texas Dist 3', 'Texas Dist 4') THEN production END) as south_texas_production
        ,SUM(CASE WHEN region in ('Texas Dist 1', 'Texas Dist 2', 'Texas Dist 3', 'Texas Dist 4') THEN dry_gas_production_yoy END) as south_texas_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Texas Dist 1', 'Texas Dist 2', 'Texas Dist 3', 'Texas Dist 4') THEN oil_rig_count END) as south_texas_oil_rig_count
        ,SUM(CASE WHEN region in ('Texas Dist 1', 'Texas Dist 2', 'Texas Dist 3', 'Texas Dist 4') THEN gas_rig_count END) as south_texas_gas_rig_count
        -- 'East Texas'
        ,SUM(CASE WHEN region in ('Texas Dist 5', 'Texas Dist 6', 'Texas Dist 7B', 'Texas Dist 9') THEN production END) as east_texas_production
        ,SUM(CASE WHEN region in ('Texas Dist 5', 'Texas Dist 6', 'Texas Dist 7B', 'Texas Dist 9') THEN dry_gas_production_yoy END) as east_texas_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Texas Dist 5', 'Texas Dist 6', 'Texas Dist 7B', 'Texas Dist 9') THEN oil_rig_count END) as east_texas_oil_rig_count
        ,SUM(CASE WHEN region in ('Texas Dist 5', 'Texas Dist 6', 'Texas Dist 7B', 'Texas Dist 9') THEN gas_rig_count END) as east_texas_gas_rig_count
        
        -- 'Mid Continent'
        ,SUM(CASE WHEN region in ('Texas Dist 10', 'Oklahoma', 'Kansas', 'Arkansas') THEN production END) as mid_con_production
        ,SUM(CASE WHEN region in ('Texas Dist 10', 'Oklahoma', 'Kansas', 'Arkansas') THEN dry_gas_production_yoy END) as mid_con_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Texas Dist 10', 'Oklahoma', 'Kansas', 'Arkansas') THEN oil_rig_count END) as mid_con_oil_rig_count
        ,SUM(CASE WHEN region in ('Texas Dist 10', 'Oklahoma', 'Kansas', 'Arkansas') THEN gas_rig_count END) as mid_con_gas_rig_count
        
        -- 'Permian'
        ,SUM(CASE WHEN region in ('Texas Dist 7C', 'Texas Dist 8', 'Texas Dist 8A', 'Permian New Mexico') THEN production END) as permian_production
        ,SUM(CASE WHEN region in ('Texas Dist 7C', 'Texas Dist 8', 'Texas Dist 8A', 'Permian New Mexico') THEN dry_gas_production_yoy END) as permian_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Texas Dist 7C', 'Texas Dist 8', 'Texas Dist 8A', 'Permian New Mexico') THEN oil_rig_count END) as permian_oil_rig_count
        ,SUM(CASE WHEN region in ('Texas Dist 7C', 'Texas Dist 8', 'Texas Dist 8A', 'Permian New Mexico') THEN gas_rig_count END) as permian_gas_rig_count
        
        -- 'San Juan'
        ,SUM(CASE WHEN region in ('Colorado San Juan', 'New Mexico San Juan') THEN production END) as san_juan_production
        ,SUM(CASE WHEN region in ('Colorado San Juan', 'New Mexico San Juan') THEN dry_gas_production_yoy END) as san_juan_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Colorado San Juan', 'New Mexico San Juan') THEN oil_rig_count END) as san_juan_oil_rig_count
        ,SUM(CASE WHEN region in ('Colorado San Juan', 'New Mexico San Juan') THEN gas_rig_count END) as san_juan_gas_rig_count
        
        -- 'Rockies'
        ,SUM(CASE WHEN region in ('Colorado Piceance', 'Colorado Denver Julesberg', 'Colorado Other', 'Montana', 'North Dakota', 'Utah Uinta', 'Utah Other', 'Other Rockies', 'Wyoming Big Horn', 'Wyoming Denver Julesberg', 'Wyoming Green/Wind/OT', 'Wyoming Powder') THEN production END) as rockies_production
        ,SUM(CASE WHEN region in ('Colorado Piceance', 'Colorado Denver Julesberg', 'Colorado Other', 'Montana', 'North Dakota', 'Utah Uinta', 'Utah Other', 'Other Rockies', 'Wyoming Big Horn', 'Wyoming Denver Julesberg', 'Wyoming Green/Wind/OT', 'Wyoming Powder') THEN dry_gas_production_yoy END) as rockies_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Colorado Piceance', 'Colorado Denver Julesberg', 'Colorado Other', 'Montana', 'North Dakota', 'Utah Uinta', 'Utah Other', 'Other Rockies', 'Wyoming Big Horn', 'Wyoming Denver Julesberg', 'Wyoming Green/Wind/OT', 'Wyoming Powder') THEN oil_rig_count END) as rockies_oil_rig_count
        ,SUM(CASE WHEN region in ('Colorado Piceance', 'Colorado Denver Julesberg', 'Colorado Other', 'Montana', 'North Dakota', 'Utah Uinta', 'Utah Other', 'Other Rockies', 'Wyoming Big Horn', 'Wyoming Denver Julesberg', 'Wyoming Green/Wind/OT', 'Wyoming Powder') THEN gas_rig_count END) as rockies_gas_rig_count

        -- 'West'
        ,SUM(CASE WHEN region in ('California', 'Other West') THEN production END) as west_production
        ,SUM(CASE WHEN region in ('California', 'Other West') THEN dry_gas_production_yoy END) as west_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('California', 'Other West') THEN oil_rig_count END) as west_oil_rig_count
        ,SUM(CASE WHEN region in ('California', 'Other West') THEN gas_rig_count END) as west_gas_rig_count

        -- 'East'
        ,SUM(CASE WHEN region in ('Kentucky', 'Michigan', 'New York', 'Ohio', 'Pennsylvania', 'Virginia', 'West Virginia', 'Other East') THEN production END) as east_production
        ,SUM(CASE WHEN region in ('Kentucky', 'Michigan', 'New York', 'Ohio', 'Pennsylvania', 'Virginia', 'West Virginia', 'Other East') THEN dry_gas_production_yoy END) as east_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Kentucky', 'Michigan', 'New York', 'Ohio', 'Pennsylvania', 'Virginia', 'West Virginia', 'Other East') THEN oil_rig_count END) as east_oil_rig_count
        ,SUM(CASE WHEN region in ('Kentucky', 'Michigan', 'New York', 'Ohio', 'Pennsylvania', 'Virginia', 'West Virginia', 'Other East') THEN gas_rig_count END) as east_gas_rig_count

        -- 'Western Canada'
        ,SUM(CASE WHEN region in ('Alberta', 'British Columbia', 'Saskatchewan') THEN production END) as western_canada_production
        ,SUM(CASE WHEN region in ('Alberta', 'British Columbia', 'Saskatchewan') THEN dry_gas_production_yoy END) as western_canada_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Alberta', 'British Columbia', 'Saskatchewan') THEN oil_rig_count END) as western_canada_oil_rig_count
        ,SUM(CASE WHEN region in ('Alberta', 'British Columbia', 'Saskatchewan') THEN gas_rig_count END) as western_canada_gas_rig_count

        -- 'East'
        -- 'Ohio'
        ,SUM(CASE WHEN region in ('Ohio') THEN production END) as ohio_production
        ,SUM(CASE WHEN region in ('Ohio') THEN dry_gas_production_yoy END) as ohio_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Ohio') THEN oil_rig_count END) as ohio_oil_rig_count
        ,SUM(CASE WHEN region in ('Ohio') THEN gas_rig_count END) as ohio_gas_rig_count
        -- 'Pennsylvania'
        ,SUM(CASE WHEN region in ('Pennsylvania') THEN production END) as pennsylvania_production
        ,SUM(CASE WHEN region in ('Pennsylvania') THEN dry_gas_production_yoy END) as pennsylvania_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Pennsylvania') THEN oil_rig_count END) as pennsylvania_oil_rig_count
        ,SUM(CASE WHEN region in ('Pennsylvania') THEN gas_rig_count END) as pennsylvania_gas_rig_count
        -- 'Northeast Pennsylvania'
        ,SUM(CASE WHEN region in ('Northeast Pennsylvania') THEN production END) as ne_pennsylvania_production
        ,SUM(CASE WHEN region in ('Northeast Pennsylvania') THEN dry_gas_production_yoy END) as ne_pennsylvania_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Northeast Pennsylvania') THEN oil_rig_count END) as ne_pennsylvania_oil_rig_count
        ,SUM(CASE WHEN region in ('Northeast Pennsylvania') THEN gas_rig_count END) as ne_pennsylvania_gas_rig_count
        -- 'Southwest Pennsylvania'
        ,SUM(CASE WHEN region in ('Southwest Pennsylvania') THEN production END) as sw_pennsylvania_production
        ,SUM(CASE WHEN region in ('Southwest Pennsylvania') THEN dry_gas_production_yoy END) as sw_pennsylvania_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Southwest Pennsylvania') THEN oil_rig_count END) as sw_pennsylvania_oil_rig_count
        ,SUM(CASE WHEN region in ('Southwest Pennsylvania') THEN gas_rig_count END) as sw_pennsylvania_gas_rig_count
        -- 'Virginia'
        ,SUM(CASE WHEN region in ('Virginia') THEN production END) as virginia_production
        ,SUM(CASE WHEN region in ('Virginia') THEN dry_gas_production_yoy END) as virginia_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Virginia') THEN oil_rig_count END) as virginia_oil_rig_count
        ,SUM(CASE WHEN region in ('Virginia') THEN gas_rig_count END) as virginia_gas_rig_count
        -- 'West Virginia'
        ,SUM(CASE WHEN region in ('West Virginia') THEN production END) as west_virginia_production
        ,SUM(CASE WHEN region in ('West Virginia') THEN dry_gas_production_yoy END) as west_virginia_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('West Virginia') THEN oil_rig_count END) as west_virginia_oil_rig_count
        ,SUM(CASE WHEN region in ('West Virginia') THEN gas_rig_count END) as west_virginia_gas_rig_count
        -- 'Other East'
        ,SUM(CASE WHEN region in ('Other East') THEN production END) as other_east_production
        ,SUM(CASE WHEN region in ('Other East') THEN dry_gas_production_yoy END) as other_east_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Other East') THEN oil_rig_count END) as other_east_oil_rig_count
        ,SUM(CASE WHEN region in ('Other East') THEN gas_rig_count END) as other_east_gas_rig_count

        -- 'Alaska'
        ,SUM(CASE WHEN region in ('Alaska') THEN production END) as alaska_production
        ,SUM(CASE WHEN region in ('Alaska') THEN dry_gas_production_yoy END) as alaska_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Alaska') THEN oil_rig_count END) as alaska_oil_rig_count
        ,SUM(CASE WHEN region in ('Alaska') THEN gas_rig_count END) as alaska_gas_rig_count

        -- 'Nova Scotia'
        ,SUM(CASE WHEN region in ('Nova Scotia') THEN production END) as nova_scotia_production
        ,SUM(CASE WHEN region in ('Nova Scotia') THEN dry_gas_production_yoy END) as nova_scotia_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('Nova Scotia') THEN oil_rig_count END) as nova_scotia_oil_rig_count
        ,SUM(CASE WHEN region in ('Nova Scotia') THEN gas_rig_count END) as nova_scotia_gas_rig_count

        -- 'United States'
        ,SUM(CASE WHEN region in ('United States') THEN production END) as united_states_production
        ,SUM(CASE WHEN region in ('United States') THEN dry_gas_production_yoy END) as united_states_dry_gas_production_yoy
        ,SUM(CASE WHEN region in ('United States') THEN oil_rig_count END) as united_states_oil_rig_count
        ,SUM(CASE WHEN region in ('United States') THEN gas_rig_count END) as united_states_gas_rig_count

    -- from genscape_v1_2025_dec_08.source_v1_gas_production_forecast
    FROM {{ ref('source_v2_genscape_gas_production_forecast') }}

    GROUP BY report_date, year, month, date
),

-- SELECT * FROM GENSCAPE_PROD_FORECAST_REGIONS
-- ORDER BY report_date DESC, year desc, month desc

-------------------------------------------------------------
-------------------------------------------------------------

REVISIONS AS (
    SELECT 
        * 
        ,ROW_NUMBER() OVER (PARTITION BY date ORDER BY report_date) as revision
    FROM GENSCAPE_PROD_FORECAST_REGIONS
),

MAX_REVISIONS AS (
    SELECT 
        * 
        ,MAX(revision) OVER (PARTITION BY date) AS max_revision
    FROM REVISIONS
),

-- SELECT * FROM MAX_REVISIONS
-- ORDER BY report_date DESC, year desc, month desc

-------------------------------------------------------------
-------------------------------------------------------------

FINAL AS (
    SELECT

        year
        ,month
        ,date
        
        -- revision
        ,report_date
        ,revision
        ,max_revision

        -- lower_48
        ,lower_48_production
        ,lower_48_dry_gas_production_yoy
        ,lower_48_oil_rig_count
        ,lower_48_gas_rig_count

         -- 'Gulf of Mexico'
        ,gulf_of_mexico_production
        ,gulf_of_mexico_dry_gas_production_yoy
        ,gulf_of_mexico_oil_rig_count
        ,gulf_of_mexico_gas_rig_count

        -- 'Gulf Coast'
        ,gulf_coast_production
        ,gulf_coast_dry_gas_production_yoy
        ,gulf_coast_oil_rig_count
        ,gulf_coast_gas_rig_count
        -- 'North Louisiana'
        ,north_louisiana_production
        ,north_louisiana_dry_gas_production_yoy
        ,north_louisiana_oil_rig_count
        ,north_louisiana_gas_rig_count
        -- 'South Louisiana'
        ,south_louisiana_production
        ,south_louisiana_dry_gas_production_yoy
        ,south_louisiana_oil_rig_count
        ,south_louisiana_gas_rig_count

        -- 'Texas' (composite: south_texas + east_texas)
        ,(COALESCE(south_texas_production, 0) + COALESCE(east_texas_production, 0)) as texas_production
        ,(COALESCE(south_texas_dry_gas_production_yoy, 0) + COALESCE(east_texas_dry_gas_production_yoy, 0)) as texas_dry_gas_production_yoy
        ,(COALESCE(south_texas_oil_rig_count, 0) + COALESCE(east_texas_oil_rig_count, 0)) as texas_oil_rig_count
        ,(COALESCE(south_texas_gas_rig_count, 0) + COALESCE(east_texas_gas_rig_count, 0)) as texas_gas_rig_count
        -- 'South Texas'
        ,south_texas_production
        ,south_texas_dry_gas_production_yoy
        ,south_texas_oil_rig_count
        ,south_texas_gas_rig_count
        -- 'East Texas'
        ,east_texas_production
        ,east_texas_dry_gas_production_yoy
        ,east_texas_oil_rig_count
        ,east_texas_gas_rig_count
        
        -- 'Mid Continent'
        ,mid_con_production
        ,mid_con_dry_gas_production_yoy
        ,mid_con_oil_rig_count
        ,mid_con_gas_rig_count
        
        -- 'Permian'
        ,permian_production
        ,permian_dry_gas_production_yoy
        ,permian_oil_rig_count
        ,permian_gas_rig_count
        
        -- 'San Juan'
        ,san_juan_production
        ,san_juan_dry_gas_production_yoy
        ,san_juan_oil_rig_count
        ,san_juan_gas_rig_count
        
        -- 'Rockies'
        ,rockies_production
        ,rockies_dry_gas_production_yoy
        ,rockies_oil_rig_count
        ,rockies_gas_rig_count

        -- 'West'
        ,west_production
        ,west_dry_gas_production_yoy
        ,west_oil_rig_count
        ,west_gas_rig_count

        -- 'East'
        ,east_production
        ,east_dry_gas_production_yoy
        ,east_oil_rig_count
        ,east_gas_rig_count

        -- 'Western Canada'
        ,western_canada_production
        ,western_canada_dry_gas_production_yoy
        ,western_canada_oil_rig_count
        ,western_canada_gas_rig_count
        
        -- 'East'
        -- 'Ohio'
        ,ohio_production
        ,ohio_dry_gas_production_yoy
        ,ohio_oil_rig_count
        ,ohio_gas_rig_count
        -- 'Pennsylvania'
        ,pennsylvania_production
        ,pennsylvania_dry_gas_production_yoy
        ,pennsylvania_oil_rig_count
        ,pennsylvania_gas_rig_count
        -- 'Northeast Pennsylvania'
        ,ne_pennsylvania_production
        ,ne_pennsylvania_dry_gas_production_yoy
        ,ne_pennsylvania_oil_rig_count
        ,ne_pennsylvania_gas_rig_count
        -- 'Southwest Pennsylvania'
        ,sw_pennsylvania_production
        ,sw_pennsylvania_dry_gas_production_yoy
        ,sw_pennsylvania_oil_rig_count
        ,sw_pennsylvania_gas_rig_count
        -- 'Virginia'
        ,virginia_production
        ,virginia_dry_gas_production_yoy
        ,virginia_oil_rig_count
        ,virginia_gas_rig_count
        -- 'West Virginia'
        ,west_virginia_production
        ,west_virginia_dry_gas_production_yoy
        ,west_virginia_oil_rig_count
        ,west_virginia_gas_rig_count
        -- 'Other East'
        ,other_east_production
        ,other_east_dry_gas_production_yoy
        ,other_east_oil_rig_count
        ,other_east_gas_rig_count

        -- 'Alaska'
        ,alaska_production
        ,alaska_dry_gas_production_yoy
        ,alaska_oil_rig_count
        ,alaska_gas_rig_count

        -- 'Nova Scotia'
        ,nova_scotia_production
        ,nova_scotia_dry_gas_production_yoy
        ,nova_scotia_oil_rig_count
        ,nova_scotia_gas_rig_count

        -- 'United States'
        ,united_states_production
        ,united_states_dry_gas_production_yoy
        ,united_states_oil_rig_count
        ,united_states_gas_rig_count

    FROM MAX_REVISIONS
)

SELECT * FROM FINAL
ORDER BY date ASC, report_date DESC