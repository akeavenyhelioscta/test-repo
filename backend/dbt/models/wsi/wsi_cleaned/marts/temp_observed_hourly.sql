{{
  config(
    materialized='incremental',
    unique_key=['datetime', 'region', 'site_id', 'station_name'],
    indexes=[
      {'columns': ['station_name', 'date'], 'type': 'btree'},
    ]
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH FINAL AS (
    SELECT * FROM {{ ref('staging_v1_temp_observed_hourly') }}
    {% if is_incremental() %}
    WHERE date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '3 days'
    {% endif %}
)

SELECT * FROM FINAL
ORDER BY datetime DESC, region, site_id
