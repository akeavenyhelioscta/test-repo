-- Per-database I/O and tuple throughput counters since the last stats reset.
-- These are cumulative; sample twice and diff to get a rate.
SELECT
    datname,
    xact_commit,
    xact_rollback,
    blks_read,
    blks_hit,
    CASE WHEN blks_hit + blks_read = 0 THEN NULL
         ELSE round(100.0 * blks_hit / (blks_hit + blks_read), 2)
    END                                  AS cache_hit_pct,
    tup_returned,
    tup_fetched,
    tup_inserted,
    tup_updated,
    tup_deleted,
    temp_files,
    pg_size_pretty(temp_bytes)           AS temp_bytes,
    deadlocks,
    stats_reset
FROM pg_stat_database
WHERE datname IS NOT NULL
ORDER BY xact_commit + xact_rollback DESC;
