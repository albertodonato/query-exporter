SELECT
  z.name AS tablespace,
  dt.contents AS type,
  z.bytes AS oracle_tablespace_bytes,
  z.max_bytes AS oracle_tablespace_max_bytes,
  z.free_bytes AS oracle_tablespace_free
FROM
  (
    SELECT
      x.name AS name,
      SUM(nvl(x.free_bytes,0)) AS free_bytes,
      SUM(x.bytes) AS bytes,
      SUM(x.max_bytes) AS max_bytes
    FROM
      (
        SELECT
          ddf.tablespace_name AS name,
          ddf.status AS status,
          ddf.bytes AS bytes,
          sum(coalesce(dfs.bytes, 0)) AS free_bytes,
          CASE
            WHEN ddf.maxbytes = 0 THEN ddf.bytes
            ELSE ddf.maxbytes
          END AS max_bytes
        FROM
          sys.dba_data_files AS ddf,
          sys.dba_tablespaces AS dt,
          sys.dba_free_space AS dfs
        WHERE ddf.tablespace_name = dt.tablespace_name
        AND ddf.file_id = dfs.file_id(+)
        GROUP BY ddf.tablespace_name, ddf.file_name, ddf.status, ddf.bytes, ddf.maxbytes
      ) AS x
    GROUP BY x.name
    UNION ALL
    SELECT
      y.name AS name,
      MAX(nvl(Y.free_bytes, 0)) AS free_bytes,
      SUM(Y.bytes) AS bytes,
      SUM(Y.max_bytes) AS max_bytes
    FROM
      (
        SELECT
          dtf.tablespace_name AS name,
          dtf.status AS status,
          dtf.bytes AS bytes,
          (
            SELECT
              ((f.total_blocks - s.tot_used_blocks) * vp.value)
            FROM
              (
                SELECT
                  tablespace_name,
                  sum(used_blocks) AS tot_used_blocks
                FROM gv$sort_segment
                WHERE tablespace_name != 'DUMMY'
                GROUP BY tablespace_name
              ) AS s,
              (
                SELECT
                  tablespace_name,
                  sum(blocks) AS total_blocks
                FROM dba_temp_files
                WHERE tablespace_name !='DUMMY'
                GROUP BY tablespace_name
              ) AS f,
              (
                SELECT
                  value
                FROM v$parameter
                WHERE name = 'db_block_size'
              ) AS vp
            WHERE f.tablespace_name = s.tablespace_name AND f.tablespace_name = dtf.tablespace_name
          ) AS free_bytes,
          CASE
            WHEN dtf.maxbytes = 0 THEN dtf.bytes
            ELSE dtf.maxbytes
          END AS max_bytes
        FROM sys.dba_temp_files AS dtf
      ) AS y
    GROUP BY y.name
  ) AS z,
  sys.dba_tablespaces AS dt
  WHERE z.name = dt.tablespace_name
