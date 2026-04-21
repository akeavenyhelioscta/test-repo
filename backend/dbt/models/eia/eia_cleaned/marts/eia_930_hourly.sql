{{
  config(
    materialized='incremental',
    unique_key=['datetime_utc', 'respondent'],
    incremental_strategy='delete+insert'
  )
}}

---------------------------
-- EIA-930 HOURLY GENERATION BY RESPONDENT
---------------------------

WITH FINAL AS (
    SELECT * FROM {{ ref('staging_v1_eia_930_hourly') }}
)

SELECT * FROM FINAL

{% if is_incremental() %}
WHERE date >= (SELECT MAX(date) - INTERVAL '10 days' FROM {{ this }})
{% endif %}
