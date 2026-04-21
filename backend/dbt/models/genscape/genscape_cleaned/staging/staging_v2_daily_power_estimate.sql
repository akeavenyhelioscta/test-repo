{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-- Staging: Genscape Daily Power Estimate
-- Passes through cleaned source data.
-- Grain: 1 row per gasday x power_burn_variable x modeltype
-------------------------------------------------------------

WITH SOURCE AS (
    SELECT * FROM {{ ref('source_v2_daily_power_estimate') }}
),

FINAL AS (
    SELECT
        gasday as gas_day
        ,power_burn_variable

        -- Genscape uses three different models depending on the data available:
            -- (1) Noms only is used for the current gas day
            -- (2) Noms + monitored is used for the previous 2 days
            -- (3) After three days, we use noms + monitored + no notice
        ,modeltype as model_type_based_on_noms
        ,MAX(modeltype) OVER (PARTITION BY gasday, power_burn_variable ORDER BY gasday DESC, power_burn_variable DESC) AS max_model_type_based_on_noms

        ,conus
        ,east
        ,midwest
        ,mountain
        ,pacific
        ,south_central

    FROM SOURCE
)

SELECT * FROM FINAL
ORDER BY gas_day DESC, power_burn_variable, model_type_based_on_noms