{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RAW BLOOMBERG DAPI TICKERS
---------------------------

WITH RAW AS (
    SELECT
        security::VARCHAR AS security,
        description::VARCHAR AS description,
        created_at::TIMESTAMP AS created_at,
        updated_at::TIMESTAMP AS updated_at
    FROM {{ source('bbg_dapi_v1', 'bbg_tickers') }}
),

FINAL AS (
    SELECT * FROM RAW
)

SELECT * FROM FINAL
