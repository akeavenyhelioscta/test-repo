-------------------------------------------------------------
-------------------------------------------------------------

WITH TRADES AS (
    SELECT * FROM {{ ref('staging_v2_marex_allocated_trades_2_product_codes') }}
)

SELECT * FROM TRADES
ORDER BY
  sftp_date DESC,
  sftp_upload_timestamp DESC,
  product_code_grouping,
  product_code_region