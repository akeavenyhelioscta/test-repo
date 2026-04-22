-- Per-table cache hit ratio. A low ratio on a high-traffic table means
-- cold reads from disk — candidate for better indexing or more RAM.
SELECT
    schemaname || '.' || relname AS relation,
    heap_blks_read,
    heap_blks_hit,
    CASE WHEN heap_blks_hit + heap_blks_read = 0 THEN NULL
         ELSE round(100.0 * heap_blks_hit / (heap_blks_hit + heap_blks_read), 2)
    END AS heap_hit_pct,
    idx_blks_read,
    idx_blks_hit
FROM pg_statio_user_tables
ORDER BY heap_blks_read + idx_blks_read DESC NULLS LAST
LIMIT 20;
