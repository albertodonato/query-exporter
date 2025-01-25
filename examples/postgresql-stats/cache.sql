SELECT
  SUM(heap_blks_read) AS pg_cache_heap_read,
  SUM(heap_blks_hit) AS pg_cache_heap_hit,
  (SUM(heap_blks_hit) / (SUM(heap_blks_hit) + SUM(heap_blks_read))) * 100 AS pg_cache_hit_ratio
FROM pg_statio_user_tables
