{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PARSE DATE COMPONENTS AND JOIN AREA LOOKUP
---------------------------

WITH CONSUMPTION_RAW AS (
    SELECT
        SPLIT_PART(src.period, '-', 1)::INTEGER AS year,
        SPLIT_PART(src.period, '-', 2)::INTEGER AS month,
        lookup.area_name_standardized,
        CASE
            WHEN src.process_name = 'Lease and Plant Fuel Consumption' THEN 'Lease and Plant Fuel'
            WHEN src.process_name = 'Pipeline Fuel Consumption' THEN 'Pipeline & Distribution Use'
            WHEN src.process_name = 'Delivered to Consumers' THEN 'Volumes Delivered to Consumers'
            WHEN src.process_name = 'Residential Consumption' THEN 'Residential'
            WHEN src.process_name = 'Commercial Consumption' THEN 'Commercial'
            WHEN src.process_name = 'Industrial Consumption' THEN 'Industrial'
            WHEN src.process_name = 'Vehicle Fuel Consumption' THEN 'Vehicle Fuel'
            WHEN src.process_name = 'Electric Power Consumption' THEN 'Electric Power'
            ELSE NULL
        END AS consumption_type_standardized,
        src.units AS consumption_unit,
        src.value AS consumption
    FROM {{ ref('source_v1_eia_nat_gas_consumption_end_use') }} src
    LEFT JOIN {{ ref('utils_v1_eia_area_name_lookup') }} lookup
        ON src.area_name = lookup.area_name
),

---------------------------
-- PIVOT CONSUMPTION TYPES INTO COLUMNS
---------------------------

FINAL AS (
    SELECT
        year,
        month,
        area_name_standardized,
        consumption_unit,
        AVG(CASE WHEN consumption_type_standardized = 'Lease and Plant Fuel' THEN consumption ELSE NULL END) AS lease_and_plant_fuel,
        AVG(CASE WHEN consumption_type_standardized = 'Pipeline & Distribution Use' THEN consumption ELSE NULL END) AS pipeline_and_distribution_use,
        AVG(CASE WHEN consumption_type_standardized = 'Volumes Delivered to Consumers' THEN consumption ELSE NULL END) AS volumes_delivered_to_consumers,
        AVG(CASE WHEN consumption_type_standardized = 'Residential' THEN consumption ELSE NULL END) AS residential,
        AVG(CASE WHEN consumption_type_standardized = 'Commercial' THEN consumption ELSE NULL END) AS commercial,
        AVG(CASE WHEN consumption_type_standardized = 'Industrial' THEN consumption ELSE NULL END) AS industrial,
        AVG(CASE WHEN consumption_type_standardized = 'Vehicle Fuel' THEN consumption ELSE NULL END) AS vehicle_fuel,
        AVG(CASE WHEN consumption_type_standardized = 'Electric Power' THEN consumption ELSE NULL END) AS electric_power
    FROM CONSUMPTION_RAW
    GROUP BY year, month, area_name_standardized, consumption_unit
)

SELECT * FROM FINAL
ORDER BY year DESC, month DESC, area_name_standardized
