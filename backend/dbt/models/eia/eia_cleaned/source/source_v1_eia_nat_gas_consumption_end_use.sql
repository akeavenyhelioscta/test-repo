{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RAW EIA NATURAL GAS CONSUMPTION BY END USE
---------------------------

WITH SOURCE AS (
    SELECT
        period::VARCHAR AS period,
        area_name::VARCHAR AS area_name,
        process_name::VARCHAR AS process_name,
        units::VARCHAR AS units,
        value::NUMERIC AS value
    FROM {{ source('eia_v1', 'nat_gas_consumption_end_use_v2_2025_dec_28') }}
)

SELECT * FROM SOURCE
