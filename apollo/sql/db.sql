-- disk_usage
SELECT *, pg_size_pretty(total_bytes) AS total,
          pg_size_pretty(index_bytes) AS INDEX,
          pg_size_pretty(toast_bytes) AS toast,
          pg_size_pretty(table_bytes) AS TABLE
FROM (SELECT *, total_bytes - index_bytes - COALESCE(toast_bytes, 0) AS table_bytes
      FROM (SELECT c.oid,
                   nspname                               AS table_schema,
                   relname                               AS TABLE_NAME,
                   c.reltuples                           AS row_estimate,
                   pg_total_relation_size(c.oid)         AS total_bytes,
                   pg_indexes_size(c.oid)                AS index_bytes,
                   pg_total_relation_size(reltoastrelid) AS toast_bytes
            FROM pg_class c
                   LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE relkind = 'r') a) a
ORDER BY total_bytes DESC;

-- useless_columns
SELECT nspname,
       relname,
       attname,
       typname,
       (stanullfrac * 100) :: INT                                             AS null_percent,
       CASE
         WHEN stadistinct >= 0 THEN stadistinct
         ELSE abs(stadistinct) * reltuples END                                AS "distinct",
       CASE 1 WHEN stakind1 THEN stavalues1 WHEN stakind2 THEN stavalues2 END AS "values"
FROM pg_class c
       JOIN pg_namespace ns ON (ns.oid = relnamespace)
       JOIN pg_attribute ON (c.oid = attrelid)
       JOIN pg_type t ON (t.oid = atttypid)
       JOIN pg_statistic ON (c.oid = starelid AND staattnum = attnum)
WHERE nspname NOT LIKE E'pg\\_%'
  AND nspname != 'information_schema'
  AND relkind = 'r'
  AND NOT attisdropped
  AND attstattarget != 0
  AND reltuples >= 100              -- ignore tables with fewer than 100 rows
  AND stadistinct BETWEEN 0 AND 1   -- 0 to 1 distinct values
ORDER BY nspname, relname, null_percent DESC;

-- wasted_bytes
SELECT current_database(),
       schemaname,
       tablename,
       /*reltuples::bigint, relpages::bigint, otta,*/
       ROUND((CASE WHEN otta = 0 THEN 0.0 ELSE sml.relpages :: FLOAT / otta END) :: NUMERIC, 1) AS tbloat,
       CASE
         WHEN relpages < otta THEN 0
         ELSE bs * (sml.relpages - otta) :: BIGINT END                                          AS wastedbytes,
       iname,
       /*ituples::bigint, ipages::bigint, iotta,*/
       ROUND((CASE
                WHEN iotta = 0 OR ipages = 0 THEN 0.0
                ELSE ipages :: FLOAT / iotta END) :: NUMERIC, 1)                                AS ibloat,
       CASE WHEN ipages < iotta THEN 0 ELSE bs * (ipages - iotta) END                           AS wastedibytes
FROM (SELECT schemaname,
             tablename,
             cc.reltuples,
             cc.relpages,
             bs,
             CEIL((cc.reltuples * ((datahdr + ma -
                                    (CASE WHEN datahdr % ma = 0 THEN ma ELSE datahdr % ma END)) + nullhdr2 + 4)) /
                  (bs - 20 :: FLOAT))                                                AS otta,
             COALESCE(c2.relname, '?')                                               AS iname,
             COALESCE(c2.reltuples, 0)                                               AS ituples,
             COALESCE(c2.relpages, 0)                                                AS ipages,
             COALESCE(CEIL((c2.reltuples * (datahdr - 12)) / (bs - 20 :: FLOAT)), 0) AS iotta -- very rough approximation, assumes all cols
      FROM (SELECT ma,
                   bs,
                   schemaname,
                   tablename,
                   (datawidth + (hdr + ma - (CASE WHEN hdr % ma = 0 THEN ma ELSE hdr % ma END))) :: NUMERIC AS datahdr,
                   (maxfracsum *
                    (nullhdr + ma - (CASE WHEN nullhdr % ma = 0 THEN ma ELSE nullhdr % ma END)))            AS nullhdr2
            FROM (SELECT schemaname,
                         tablename,
                         hdr,
                         ma,
                         bs,
                         SUM((1 - null_frac) * avg_width)         AS datawidth,
                         MAX(null_frac)                           AS maxfracsum,
                         hdr + (SELECT 1 + COUNT(*) / 8
                                FROM pg_stats s2
                                WHERE null_frac <> 0
                                  AND s2.schemaname = s.schemaname
                                  AND s2.tablename = s.tablename) AS nullhdr
                  FROM pg_stats s,
                       (SELECT (SELECT current_setting('block_size') :: NUMERIC) AS bs,
                               CASE
                                 WHEN SUBSTRING(v, 12, 3) IN ('8.0', '8.1', '8.2') THEN 27
                                 ELSE 23 END                                     AS hdr,
                               CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END         AS ma
                        FROM (SELECT version() AS v) AS foo) AS constants
                  GROUP BY 1, 2, 3, 4, 5) AS foo) AS rs
             JOIN pg_class cc ON cc.relname = rs.tablename
             JOIN pg_namespace nn
               ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
             LEFT JOIN pg_index i ON indrelid = cc.oid
             LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid) AS sml
ORDER BY wastedbytes DESC;

-- remove_duplicates
DELETE
FROM cache.cache T1
    USING cache.cache T2
WHERE T1.ctid < T2.ctid
  AND T1.content = T2.content;

-- vacuum
VACUUM FULL apollo.market;
VACUUM FULL apollo.social;
VACUUM FULL apollo.trend;
VACUUM FULL apollo.messages;
VACUUM FULL apollo.cache;
