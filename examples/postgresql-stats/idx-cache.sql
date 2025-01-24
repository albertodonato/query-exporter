SELECT
  SUM(idx_blks_read) AS pg_idx_cache_read,
  SUM(idx_blks_hit) AS pg_idx_cache_hit,
  ((SUM(idx_blks_hit) - SUM(idx_blks_read)) / SUM(idx_blks_hit)) * 100 AS pg_idx_cache_hit_ratio
FROM pg_statio_user_indexes
