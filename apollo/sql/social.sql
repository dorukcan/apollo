SELECT *
FROM apollo.social;

SELECT site_name, COUNT(*)
FROM apollo.social
GROUP BY site_name
ORDER BY COUNT(*) DESC;

SELECT site_name,
       metric_name,
       COUNT(*)             AS total_count,
       COUNT(DISTINCT code) AS unique_item_count,
       SUM(CASE
             WHEN metric_value IS NULL THEN 1
             ELSE 0 END)    AS null_count
FROM apollo.social
GROUP BY site_name, metric_name
ORDER BY total_count DESC;

WITH _videos AS (SELECT code, metric_value, created_at
                 FROM social
                 WHERE site_name = 'social.youtube'
                   AND metric_name = 'view_count'
                   AND type_key = 'video')

SELECT code, metric_value, created_at
FROM _videos
ORDER BY metric_value DESC;

SELECT code,
       metric_name,
       metric_value,
       created_at
FROM apollo.social
WHERE site_name = 'social.play_store'
  AND metric_name IN ('rating_value', 'review_count')
  AND code = 'com.whatsapp'
ORDER BY code, metric_name, created_at;