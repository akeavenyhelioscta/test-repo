# .db_monitoring

One SQL file = one runnable query against the Azure Postgres instance.
Grouped by topic. Run via the `azure-postgres` MCP tool or `psql -f <file>`.

## connections/
- `capacity.sql` — live session counts vs. `max_connections`
- `by_client.sql` — sessions grouped by app / user / database / client
- `idle_in_transaction.sql` — idle-in-txn sessions older than 1 minute

## sessions/
- `running_work.sql` — all non-idle sessions with full client/query detail
- `long_running_queries.sql` — active queries over a threshold (default 30s)

## locks/
- `blocking_pairs.sql` — blocked session + the session blocking it
- `all_locks.sql` — full lock table (use when blocking_pairs is empty)

## indexes/
- `unused.sql` — non-unique, non-PK indexes with zero scans
- `all_by_usage.sql` — every index ordered by scan count (sanity check)
- `seq_scan_candidates.sql` — large tables with heavy sequential scans
- `duplicates.sql` — indexes with identical column signatures

## storage/
- `databases.sql` — size per database
- `schemas.sql` — size per schema in the current database
- `top_tables.sql` — top 50 tables by total (heap + index + toast) size
- `tablespaces.sql` — tablespace usage

## throughput/
- `per_database.sql` — commits, rollbacks, cache hit %, temp files, deadlocks
- `top_seq_scan_tables.sql` — tables reading the most tuples via seq scans
- `cache_hit_per_table.sql` — heap/index hit ratio per table

## vacuum/
- `dead_tuples.sql` — tables with the most dead tuples
- `never_analyzed.sql` — tables the planner has no statistics for
- `running_vacuum.sql` — currently executing vacuum / analyze workers
- `xid_age.sql` — transaction ID age per database (wraparound risk)
