SELECT
  name,
  total_mb * 1024 * 1024 AS oracle_asm_diskgroup_total,
  free_mb * 1024 * 1024 AS oracle_asm_diskgroup_free
FROM v$asm_diskgroup
