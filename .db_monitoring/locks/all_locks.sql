-- All current locks. Use when blocking_pairs returns nothing but you still
-- suspect contention (advisory locks, non-Lock waits, ungranted lock queue).
SELECT
    l.locktype,
    l.mode,
    l.granted,
    l.relation::regclass AS relation,
    l.transactionid,
    l.virtualxid,
    a.pid,
    a.usename,
    a.application_name,
    a.state,
    now() - a.query_start AS duration,
    a.query
FROM pg_locks l
LEFT JOIN pg_stat_activity a ON a.pid = l.pid
WHERE a.backend_type = 'client backend' OR a.backend_type IS NULL
ORDER BY l.granted, duration DESC NULLS LAST;
