-- Idle-in-transaction sessions older than 1 minute. These hold locks and
-- bloat row versions; common signal of a connection-pool or client bug.
SELECT
    pid,
    usename,
    application_name,
    client_addr,
    state,
    now() - xact_start  AS txn_age,
    now() - state_change AS idle_age,
    query
FROM pg_stat_activity
WHERE state IN ('idle in transaction', 'idle in transaction (aborted)')
  AND now() - state_change > interval '1 minute'
ORDER BY idle_age DESC;
