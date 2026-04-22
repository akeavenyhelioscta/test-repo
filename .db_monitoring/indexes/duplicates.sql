-- Duplicate indexes (same table, same column list). Usually safe to drop
-- one of each pair, keeping the uniquely-constrained one if present.
SELECT
    pg_size_pretty(sum(pg_relation_size(idx))::bigint) AS combined_size,
    array_agg(idx::regclass)                           AS indexes
FROM (
    SELECT
        indexrelid AS idx,
        (indrelid::text || ' ' || indclass::text || ' ' || indkey::text || ' ' ||
         coalesce(indexprs::text, '') || ' ' || coalesce(indpred::text, '')) AS sig
    FROM pg_index
) sub
GROUP BY sig
HAVING count(*) > 1
ORDER BY sum(pg_relation_size(idx)) DESC;
