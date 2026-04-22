-- Live session counts vs. server limits. Use this first to see whether you
-- are near max_connections and how sessions are split across states.
SELECT
    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections')             AS max_connections,
    (SELECT setting::int FROM pg_settings WHERE name = 'superuser_reserved_connections') AS superuser_reserved,
    count(*)                                                              AS total_sessions,
    count(*) FILTER (WHERE state = 'active')                              AS active,
    count(*) FILTER (WHERE state = 'idle')                                AS idle,
    count(*) FILTER (WHERE state = 'idle in transaction')                 AS idle_in_txn,
    count(*) FILTER (WHERE state = 'idle in transaction (aborted)')       AS idle_in_txn_aborted,
    count(*) FILTER (WHERE wait_event IS NOT NULL)                        AS waiting
FROM pg_stat_activity
WHERE backend_type = 'client backend';
