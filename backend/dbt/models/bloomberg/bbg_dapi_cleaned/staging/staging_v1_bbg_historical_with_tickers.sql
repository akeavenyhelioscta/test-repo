{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- JOIN HISTORICAL TO TICKERS
---------------------------

WITH HISTORICAL AS (
    SELECT * FROM {{ ref('source_v1_bbg_historical') }}
),

TICKERS AS (
    SELECT * FROM {{ ref('source_v1_bbg_tickers') }}
),

---------------------------
-- ENRICH WITH TICKER DESCRIPTION
---------------------------

JOINED AS (
    SELECT
        h.security,
        t.description,
        h.date,
        h.snapshot_at,
        h.data_type,
        h.value,
        h.created_at,
        h.updated_at
    FROM HISTORICAL AS h
    LEFT JOIN TICKERS AS t
        ON h.security = t.security
),

---------------------------
-- DEDUPLICATE AT GRAIN
-- Grain: (security, date, snapshot_at, data_type)
-- Keep row with latest updated_at; break ties with created_at DESC
---------------------------

DEDUPLICATED AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY security, date, snapshot_at, data_type
            ORDER BY updated_at DESC, created_at DESC
        ) AS row_rank
    FROM JOINED
),

DEDUPED AS (
    SELECT
        security,
        description,
        date,
        snapshot_at,
        data_type,
        value,
        created_at,
        updated_at
    FROM DEDUPLICATED
    WHERE row_rank = 1
),

---------------------------
-- REVISION TRACKING
-- Partition: (date, security, description, data_type)
-- revision = 1 is oldest snapshot; revision = max_revision is latest
---------------------------

REVISIONS AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date, security, description, data_type
            ORDER BY snapshot_at
        ) AS revision
    FROM DEDUPED
),

MAX_REVISIONS AS (
    SELECT
        *,
        MAX(revision) OVER (
            PARTITION BY date, security, description, data_type
        ) AS max_revision
    FROM REVISIONS
),

FINAL AS (
    SELECT * FROM MAX_REVISIONS
)

SELECT * FROM FINAL
