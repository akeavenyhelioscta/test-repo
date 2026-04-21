{{
  config(
    materialized='incremental',
    unique_key=['date', 'region'],
    indexes=[
      {'columns': ['date', 'region'], 'unique': True}
    ]
  )
}}

---------------------------
-- DAILY OBSERVED WDD
---------------------------

WITH FINAL AS (
    SELECT * FROM {{ ref('source_v1_daily_observed_wdd') }}
    {% if is_incremental() %}
    WHERE date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '7 days'
    {% endif %}
)

SELECT * FROM FINAL
ORDER BY date DESC, region
