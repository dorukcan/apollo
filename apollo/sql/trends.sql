SELECT *
FROM apollo.trend;

SELECT site_name, COUNT(*)
FROM apollo.trend
GROUP BY site_name
ORDER BY COUNT(*) DESC;