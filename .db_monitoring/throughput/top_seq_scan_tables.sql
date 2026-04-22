-- Top 20 tables by sequential-scan tuple traffic. High seq_tup_read is
-- often the root cause of high read throughput or CPU.
SELECT
    schemaname || '.' || relname AS relation,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_live_tup,
    n_dead_tup
FROM pg_stat_user_tables
ORDER BY seq_tup_read DESC NULLS LAST
LIMIT 20;
