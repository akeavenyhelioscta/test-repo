{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH COMBINED AS (
    SELECT * FROM {{ ref('source_v5_nav_positions_agr') }}
    UNION ALL
    SELECT * FROM {{ ref('source_v5_nav_positions_moross') }}
    UNION ALL
    SELECT * FROM {{ ref('source_v5_nav_positions_pnt') }}
    UNION ALL
    SELECT * FROM {{ ref('source_v5_nav_positions_titan') }}
)

SELECT * FROM COMBINED
ORDER BY sftp_date desc