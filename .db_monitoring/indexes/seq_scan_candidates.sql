-- Tables doing many sequential scans on large relations — candidates for an
-- index. Read seq_tup_read as the "work the planner had to do" signal.
SELECT
    schemaname || '.' || relname    AS table,
    seq_scan,
    seq_tup_read,
    idx_scan,
    n_live_tup,
    pg_size_pretty(pg_relation_size(relid)) AS table_size
FROM pg_stat_user_tables
WHERE seq_scan > 0
  AND n_live_tup > 10000
ORDER BY seq_tup_read DESC
LIMIT 25;
