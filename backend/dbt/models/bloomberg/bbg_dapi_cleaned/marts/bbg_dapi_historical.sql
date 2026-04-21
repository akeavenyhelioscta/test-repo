{{
  config(
    materialized='view'
  )
}}

---------------------------
-- BLOOMBERG DAPI HISTORICAL MART
---------------------------

WITH FINAL AS (
    SELECT
        date,
        snapshot_at,
        revision,
        max_revision,
        security,
        description,
        data_type,
        value
    FROM {{ ref('staging_v1_bbg_historical_with_tickers') }}
)

SELECT * FROM FINAL
ORDER BY date DESC, snapshot_at DESC, security, description, data_type
