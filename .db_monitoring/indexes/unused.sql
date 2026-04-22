-- Indexes that have never been used (excluding unique / primary-key indexes,
-- which are still needed for constraint enforcement even if never read).
SELECT
    s.schemaname || '.' || s.relname        AS table,
    s.indexrelname                          AS index,
    pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
    s.idx_scan
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.idx_scan = 0
  AND NOT i.indisunique
  AND NOT i.indisprimary
ORDER BY pg_relation_size(s.indexrelid) DESC;
