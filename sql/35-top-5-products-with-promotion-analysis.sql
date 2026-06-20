
-- ---- Problem ----
-- An e-commerce platform tracks sales and promotions. Your task is to identify the top 5
-- products by total revenue and analyze how much of each product's revenue came from
-- promotion periods versus non-promotion periods.
--
-- A sale is considered promotional if the sale_date falls between any promotion's
-- start_date and end_date for that product (inclusive). If a sale does not fall within any
-- promotion period, it is non-promotional. Calculate total, promotional, and non-promotional
-- revenue for each product, then return only the top 5 by total revenue.
--
-- Schema columns: products.product_id, products.product_name, products.category,
-- promotions.promo_id, promotions.product_id, promotions.start_date,
-- promotions.end_date, promotions.discount_pct, sales.sale_id, sales.product_id,
-- sales.quantity, sales.sale_date, sales.revenue
--
-- Output columns: product_name, total_revenue, promo_revenue, non_promo_revenue
--
-- Sort by total_revenue descending
--
-- Schema: 3 tables
-- products   - 3 cols
-- promotions - 5 cols
-- sales      - 5 cols

-- ---- Examples ----
-- Example 1

-- Input:
-- products:
-- product_id | product_name | category
-- 1          | Product A    | Electronics
-- 2          | Product B    | Electronics
-- 3          | Product C    | Clothing
-- 4          | Product D    | Clothing
-- 5          | Product E    | Home

-- promotions:
-- promo_id | product_id | start_date | end_date   | discount_pct
-- 1        | 1          | 2024-01-01 | 2024-01-05 | 10
-- 2        | 2          | 2024-01-04 | 2024-01-08 | 15
-- 3        | 3          | 2024-01-01 | 2024-01-10 | 20

-- sales:
-- sale_id | product_id | quantity | sale_date  | revenue
-- 1       | 1          | 10       | 2024-01-01 | 500.00
-- 2       | 1          | 5        | 2024-01-10 | 250.00
-- 3       | 2          | 20       | 2024-01-05 | 800.00
-- 4       | 3          | 15       | 2024-01-03 | 600.00
-- 5       | 4          | 8        | 2024-01-08 | 400.00

-- Output:
-- product_name | total_revenue | promo_revenue | non_promo_revenue
-- Product B    | 1300.0        | 800.0         | 500.0
-- Product C    | 800.0         | 600.0         | 200.0
-- Product E    | 800.0         | 0.0           | 800.0
-- Product A    | 750.0         | 500.0         | 250.0
-- Product D    | 700.0         | 0.0           | 700.0

-- Explanation: The output shows 5 row(s) derived by applying the required transformations
-- to the input data.

-- ---- Constraints ----
-- - A sale matches a promotion if sale_date is between start_date and end_date (inclusive)
-- - Products with no matching promotion have 0.0 promo revenue
-- - Round all revenue values to 2 decimal places
-- - Return only the top 5 products by total revenue
-- - Sort by total_revenue descending

--- write SQL Query here

WITH 
    status_promotion AS
(
SELECT 
    p.product_id,
    p.product_name,
    s.revenue,
    CASE 
        WHEN s.sale_date >= pp.start_date AND s.sale_date <= pp.end_date THEN 'Promotional'
        ELSE 'No-promotional'
    END AS status_promotion
FROM sales AS s
INNER JOIN products AS p 
ON p.product_id = s.product_id
LEFT JOIN promotions AS pp 
ON p.product_id = pp.product_id
    )
SELECT 
    sp.product_name,
    SUM(sp.revenue) AS total_revenue,
    SUM(CASE WHEN status_promotion = 'Promotional' THEN sp.revenue ELSE 0.0 END) AS promo_revenue,
    SUM(CASE WHEN status_promotion = 'No-promotional' THEN sp.revenue ELSE 0.0 END) AS non_promo_revenue
FROM status_promotion AS sp
GROUP BY
    sp.product_id,
    sp.product_name
ORDER BY total_revenue DESC, product_name ASC
LIMIT 5