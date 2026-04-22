-- Tables that have NEVER been (auto)analyzed — the planner is guessing
-- statistics for these and may pick bad plans.
SELECT
    schemaname || '.' || relname AS relation,
    n_live_tup,
    n_mod_since_analyze
FROM pg_stat_user_tables
WHERE last_analyze IS NULL
  AND last_autoanalyze IS NULL
ORDER BY n_live_tup DESC NULLS LAST;
