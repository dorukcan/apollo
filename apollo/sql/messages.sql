SELECT *
FROM apollo.messages
ORDER BY created_at DESC;

-- label_frequency
SELECT label, DATE_TRUNC('HOUR', created_at) + INTERVAL '3 HOURS' AS time_bin, COUNT(*) AS count_val
FROM apollo.messages
GROUP BY label, time_bin
ORDER BY time_bin DESC;

-- level_frequency
SELECT level, DATE_TRUNC('HOUR', created_at) + INTERVAL '3 HOURS' AS time_bin, COUNT(*)AS count_val
FROM apollo.messages
GROUP BY level, time_bin
ORDER BY time_bin DESC;

-- per_minute
SELECT DATE_TRUNC('MINUTE', created_at) + INTERVAL '3 HOURS' AS time_bin, COUNT(*) AS total_count
FROM apollo.messages
GROUP BY time_bin
ORDER BY time_bin DESC;