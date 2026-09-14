-- =========================================================
-- Analytics queries for SQLite database: food_delivery.db
-- Table: orders
-- Database: data/food_delivery.db (SQLite)
-- Description: Analytical queries over the orders table.
--   Revenue-related queries filter to order_status = 'Delivered'
--   (case-sensitive match). order_month is YYYY-MM so string
--   ordering is chronological. Query 10 intentionally aggregates
--   across all delivery statuses (no Delivered filter) to show
--   delivery performance by type.
-- =========================================================

-- =========================================================
-- 1. How many orders do we have?
-- =========================================================

SELECT
    COUNT(*) AS total_orders
FROM orders;


-- =========================================================
-- 2. Total revenue
-- =========================================================

SELECT
    SUM(total) AS total_revenue
FROM orders
WHERE order_status = 'Delivered';


-- =========================================================
-- 3. Revenue by city
-- =========================================================

SELECT
    city,
    COUNT(*) AS orders,
    SUM(total) AS revenue
FROM orders
WHERE order_status = 'Delivered'
GROUP BY city
ORDER BY revenue DESC;


-- =========================================================
-- 4. Revenue by month
-- =========================================================

SELECT
    order_month,
    COUNT(*) AS orders,
    SUM(total) AS revenue
FROM orders
WHERE order_status = 'Delivered'
GROUP BY order_month
ORDER BY order_month;


-- =========================================================
-- 5. Top customers by spending
-- =========================================================

SELECT
    customer_id,
    COUNT(*) AS order_count,
    SUM(total) AS total_spent
FROM orders
WHERE order_status = 'Delivered'
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;


-- =========================================================
-- 6. Rank customers by total spending
-- =========================================================

SELECT
    customer_id,
    SUM(total) AS total_spent,
    RANK() OVER (ORDER BY SUM(total) DESC) AS customer_rank
FROM orders
WHERE order_status = 'Delivered'
GROUP BY customer_id
ORDER BY customer_rank;


-- =========================================================
-- 7. Most valuable customer in each city
-- =========================================================

WITH customer_city_revenue AS (
    SELECT
        city,
        customer_id,
        SUM(total) AS revenue
    FROM orders
    WHERE order_status = 'Delivered'
    GROUP BY
        city,
        customer_id
)
SELECT
    city,
    customer_id,
    revenue
FROM (
    SELECT
        city,
        customer_id,
        revenue,
        ROW_NUMBER() OVER (
            PARTITION BY city
            ORDER BY revenue DESC
        ) AS rn
    FROM customer_city_revenue
) AS ranked
WHERE rn = 1
ORDER BY revenue DESC;


-- =========================================================
-- 8. Average preparation time by restaurant
-- =========================================================

SELECT
    restaurant_name,
    COUNT(*) AS completed_orders,
    ROUND(AVG(kpt_duration_minutes), 2) AS avg_kpt_minutes
FROM orders
WHERE
    order_status = 'Delivered'
    AND kpt_duration_minutes IS NOT NULL
GROUP BY restaurant_name
ORDER BY avg_kpt_minutes DESC;


-- =========================================================
-- 9. Running revenue by month
-- =========================================================

WITH monthly_revenue AS (
    SELECT
        order_month,
        SUM(total) AS monthly_revenue
    FROM orders
    WHERE order_status = 'Delivered'
    GROUP BY order_month
)
SELECT
    order_month,
    monthly_revenue,
    SUM(monthly_revenue) OVER (
        ORDER BY order_month
        ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS running_revenue
FROM monthly_revenue
ORDER BY order_month;


-- =========================================================
-- 10. Delivery performance by delivery type
-- NOTE: No WHERE order_status = 'Delivered' filter here —
--       intentionally aggregates across all statuses to
--       compare delivery types end-to-end.
-- =========================================================

SELECT
    delivery_type,
    COUNT(*) AS orders,
    ROUND(AVG(distance_km), 2) AS avg_distance_km,
    ROUND(AVG(total_pre_delivery_minutes), 2) AS avg_pre_delivery_minutes
FROM orders
GROUP BY delivery_type
ORDER BY orders DESC;
