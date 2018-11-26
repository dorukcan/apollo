-- stats per hour (continuous)
WITH _series AS (SELECT DATE_TRUNC('HOUR', date_series) AS date_value
                 FROM generate_series(NOW() - INTERVAL '1 week', NOW(), '1 HOUR') date_series),
     _cache AS (SELECT SUM(octet_length(cache_content)) AS total_size,
                       COUNT(*)                         AS count_value,
                       date_trunc('HOUR', created_at)   AS date_value
                FROM apollo.cache
                GROUP BY date_value
                ORDER BY date_value ASC)

SELECT _series.date_value + INTERVAL '3 hours' AS date_value, total_size / 1024 / 1024 AS total_size, count_value
FROM _series
       LEFT JOIN _cache ON _cache.date_value = _series.date_value;

-- stats per minute per hostname (continuous)
WITH _series AS (SELECT DATE_TRUNC('MINUTE', date_series) AS date_value
                 FROM generate_series(NOW() - INTERVAL '24 hours', NOW(), '1 minute') date_series),
     _cache AS (SELECT substring(cache_key, '^http.?://(.*?)(?:[/|\{]).*?$') AS hostname,
                       SUM(octet_length(cache_content))                      AS total_size,
                       COUNT(*)                                              AS count_value,
                       date_trunc('MINUTE', created_at)                      AS date_value
                FROM apollo.cache
                WHERE cache_key LIKE 'http%'
                GROUP BY hostname, date_value
                ORDER BY date_value ASC)

SELECT hostname,
       _series.date_value + INTERVAL '3 hours' AS date_value,
       100 * total_size / 16.9                 AS total_size,
       count_value
FROM _series
       LEFT JOIN _cache ON _cache.date_value = _series.date_value;

-- stats per minute per hostname
SELECT substring(cache_key, '^http.?://(.*?)(?:[/|\{]).*?$') AS hostname,
       AVG(octet_length(cache_content) / 1024)               AS total_size,
       COUNT(*)                                              AS count_value,
       date_trunc('MINUTE', created_at)                      AS date_value
FROM apollo.cache
WHERE cache_key LIKE 'http%'
GROUP BY hostname, date_value
ORDER BY date_value ASC;

-- content size distribution
SELECT (octet_length(cache_content) / 1024) :: INT AS size_bin, COUNT(*) AS count_value
FROM apollo.cache
GROUP BY size_bin
ORDER BY size_bin ASC;

-- cache by domain
SELECT substring(cache_key FROM '.*://([^/]*)')       AS domain_name,
       MIN(OCTET_LENGTH(cache_content)) / 1024        AS min_kb,
       MAX(OCTET_LENGTH(cache_content)) / 1024        AS max_kb,
       AVG(OCTET_LENGTH(cache_content)) / 1024        AS avg_kb,
       SUM(OCTET_LENGTH(cache_content)) / 1024 / 1024 AS sum_mb,
       COUNT(*)                                       AS count_val
FROM apollo.cache
GROUP BY domain_name
ORDER BY COUNT(*) DESC;