SELECT
  relname,
  seq_scan AS pg_table_seq_scan,
  seq_tup_read AS pg_table_seq_tup_read,
  idx_scan AS pg_table_idx_scan,
  idx_tup_fetch AS pg_table_idx_tup_fetch,
  n_tup_ins AS pg_table_tup_insert,
  n_tup_upd AS pg_table_tup_update,
  n_tup_del AS pg_table_tup_delete,
  n_tup_hot_upd AS pg_table_tup_hot_update,
  n_live_tup AS pg_table_live_tup,
  n_dead_tup AS pg_table_dead_tup,
  vacuum_count AS pg_table_vacuum,
  autovacuum_count AS pg_table_autovacuum,
  analyze_count AS pg_table_analyze,
  autoanalyze_count AS pg_table_autoanalyze
FROM pg_stat_user_tables
