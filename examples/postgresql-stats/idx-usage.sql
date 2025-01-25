SELECT
  relname,
  100 * idx_scan / (seq_scan + idx_scan) AS pg_idx_usage_percent,
  n_live_tup AS pg_idx_usage_rows
FROM pg_stat_user_tables
WHERE seq_scan + idx_scan > 0
