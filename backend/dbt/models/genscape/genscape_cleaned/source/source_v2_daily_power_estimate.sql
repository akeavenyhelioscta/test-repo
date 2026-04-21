{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-- Source: Genscape Daily Power Estimate
-- Cleans raw columns and casts types.
-- Grain: 1 row per gasday x power_burn_variable x modeltype
-------------------------------------------------------------

WITH SOURCE AS (
    SELECT
        gasday::DATE AS gasday
        ,power_burn_variable::VARCHAR AS power_burn_variable
        ,modeltype::VARCHAR AS modeltype
        ,conus::NUMERIC AS conus
        ,east::NUMERIC AS east
        ,midwest::NUMERIC AS midwest
        ,mountain::NUMERIC AS mountain
        ,pacific::NUMERIC AS pacific
        ,south_central::NUMERIC AS south_central
    FROM {{ source('genscape_v2', 'daily_power_estimate') }}
),

FINAL AS (
    SELECT * FROM SOURCE
)

SELECT * FROM FINAL