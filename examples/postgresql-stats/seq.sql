SELECT
  relname,
  blks_read AS pg_seq_blks_read,
  blks_hit AS pg_seq_blks_hit
FROM pg_statio_user_sequences
