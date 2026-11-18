-- name: monthly_revenue_trend
SELECT order_month,
       COUNT(*)                       AS orders,
       ROUND(SUM(revenue), 2)         AS revenue,
       ROUND(SUM(revenue) - LAG(SUM(revenue)) OVER (ORDER BY order_month), 2) AS mom_change
FROM fact_sales
GROUP BY order_month
ORDER BY order_month;

-- name: top3_products_per_region
SELECT region_name, product_name, revenue, rnk
FROM (
    SELECT r.region_name,
           p.product_name,
           ROUND(SUM(f.revenue), 2) AS revenue,
           RANK() OVER (PARTITION BY r.region_name ORDER BY SUM(f.revenue) DESC) AS rnk
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_region  r ON f.region_id  = r.region_id
    GROUP BY r.region_name, p.product_name
) ranked
WHERE rnk <= 3
ORDER BY region_name, rnk;

-- name: category_revenue_share
SELECT p.category,
       ROUND(SUM(f.revenue), 2) AS revenue,
       ROUND(100.0 * SUM(f.revenue) / SUM(SUM(f.revenue)) OVER (), 2) AS pct_of_total
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;

-- name: top_repeat_customers
SELECT c.customer_name,
       COUNT(*)                 AS orders,
       ROUND(SUM(f.revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_name
HAVING COUNT(*) >= 5
ORDER BY revenue DESC
LIMIT 10;

-- name: orphan_check_customers
SELECT COUNT(*) AS orphan_rows
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
