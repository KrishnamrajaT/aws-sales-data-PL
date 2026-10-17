-- Glue Crawler run ayyaka, ee tables Glue Data Catalog lo `sales_db` database lo vastayi.
-- Athena console lo database = sales_db select chesi ee queries run cheyandi.

-- 1. Total revenue (only delivered orders)
SELECT SUM(total_amount) AS total_revenue
FROM sales
WHERE status = 'DELIVERED';

-- 2. Month-wise revenue
SELECT order_year, order_month, SUM(total_amount) AS revenue
FROM sales
WHERE status = 'DELIVERED'
GROUP BY order_year, order_month
ORDER BY order_year, order_month;

-- 3. City-wise revenue
SELECT city, COUNT(*) AS orders, SUM(total_amount) AS revenue
FROM sales
WHERE status = 'DELIVERED'
GROUP BY city
ORDER BY revenue DESC;

-- 4. Cancelled / returned orders percentage
SELECT status,
       COUNT(*) AS cnt,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM sales
GROUP BY status;

-- 5. Top 3 customers
SELECT customer_name, city, total_spent, rank
FROM top_customers
WHERE rank <= 3
ORDER BY rank;

-- 6. Partition pruning example (only Feb 2025 data scan avutundi -> cheap)
SELECT * FROM sales
WHERE order_year = 2025 AND order_month = 2;
