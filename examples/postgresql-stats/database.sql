SELECT
  datname AS datname,
  numbackends AS pg_db_numbackends,
  xact_commit AS pg_db_xact_commit,
  xact_rollback AS pg_db_xact_rollback,
  blks_read AS pg_db_blks_read,
  blks_hit AS pg_db_blks_hit,
  tup_returned AS pg_db_tup_returned,
  tup_fetched AS pg_db_tup_fetched,
  tup_inserted AS pg_db_tup_inserted,
  tup_updated AS pg_db_tup_updated,
  tup_deleted AS pg_db_tup_deleted,
  conflicts AS pg_db_conflicts,
  temp_bytes AS pg_db_temp_bytes,
  deadlocks AS pg_db_deadlocks,
  blk_read_time AS pg_db_blk_read_time,
  blk_write_time AS pg_db_blk_write_time
FROM pg_stat_database
