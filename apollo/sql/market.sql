SELECT *
FROM apollo.market;

-- group_by_site_name
SELECT site_name, COUNT(*)
FROM apollo.market
GROUP BY site_name
ORDER BY COUNT(*) DESC;

-- price_histogram
WITH _sites AS (SELECT site_name, COUNT(*) AS total_product_count FROM apollo.market GROUP BY site_name),
     products AS (SELECT site_name AS site_name, (price / 10) :: INT * 10 AS price_bin, COUNT(*) AS product_count
                  FROM apollo.market
                  GROUP BY site_name, price_bin
                  ORDER BY price_bin ASC)

SELECT products.*, products.product_count * 100 / _sites.total_product_count AS percentage
FROM products
       INNER JOIN _sites ON _sites.site_name = products.site_name;

-- avg prices per brand per category
SELECT site_name,
       category,
       AVG(price) AS avg_price,
       COUNT(*)   AS count_val
FROM apollo.market
WHERE LENGTH(category) > 0
GROUP BY site_name, category
HAVING COUNT(*) > 5
ORDER BY site_name, avg_price ASC;

-- crawling rate per minute
SELECT DATE_TRUNC('MINUTE', created_at) AS time_bin, COUNT(*)
FROM apollo.market
GROUP BY time_bin
ORDER BY time_bin DESC;

-- products with various prices
SELECT code,
       COUNT(*) AS count_val,
       max(price),
       min(price),
       array_agg(created_at)
FROM apollo.market
GROUP BY code
ORDER BY count_val DESC;

-- crawler stats
SELECT site_name, COUNT(*) AS new_count, DATE_TRUNC('HOUR', created_at) + INTERVAL '3 HOURS' AS time_bin
FROM apollo.market
GROUP BY site_name, time_bin
ORDER BY site_name ASC, time_bin ASC;