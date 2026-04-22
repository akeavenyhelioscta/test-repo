-- All indexes ordered by usage ascending. Use as a sanity check before
-- dropping anything flagged by unused.sql (counter resets, recent rebuilds).
SELECT
    s.schemaname || '.' || s.relname        AS table,
    s.indexrelname                          AS index,
    i.indisunique                           AS is_unique,
    i.indisprimary                          AS is_pk,
    pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
    s.idx_scan,
    s.idx_tup_read,
    s.idx_tup_fetch
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
ORDER BY s.idx_scan ASC, pg_relation_size(s.indexrelid) DESC;
