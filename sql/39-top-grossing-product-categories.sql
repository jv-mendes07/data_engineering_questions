-- Top Grossing Product Categories
-- Difficulty: MEDIUM | Companies

-- ---- Problem ----
-- Write a query to identify the top two products with the highest total spend within each
-- category in the year 2022. Use the table provided below, and your output should include
-- the category, product, and total spend.
--
-- Schema columns:
-- gi_product_spend: category, product, user_id, spend, transaction_date
--
-- Output columns: category, product, total_spend

-- ---- Examples ----
-- Example 1

-- Input:
-- gi_product_spend:
-- category    | product         | user_id | spend  | transaction_date
-- appliance   | refrigerator    | 123     | 299.99 | 2022-03-02 12:00:00
-- appliance   | refrigerator    | 165     | 246    | 2021-12-26 12:00:00
-- appliance   | washing machine | 123     | 219.8  | 2022-03-02 12:00:00
-- electronics | vacuum          | 178     | 152    | 2022-04-05 12:00:00

-- Output:
-- category    | product         | total_spend
-- appliance   | refrigerator    | 299.99
-- appliance   | washing machine | 219.8
-- electronics | vacuum          | 341
-- electronics | wireless headset| 249.9

-- ---- Constraints ----
-- - Handle NULL values appropriately
-- - Return results matching the expected output schema and order


-- Write query here
-- TABLE NAME: `gi_product_spend`

WITH
    products_total_spend AS (
    SELECT
        category,
        product,
        SUM(spend) AS total_spend
    FROM gi_product_spend
WHERE EXTRACT(YEAR FROM transaction_date) = 2022
GROUP BY 
        category,
        product
    ),
    rank_products_total_spend AS (
SELECT 
    category,
    product,
    total_spend,
    RANK() OVER (PARTITION BY category ORDER BY total_spend DESC) AS rank
FROM products_total_spend
    )
SELECT 
    category,
    product,
    total_spend
FROM rank_products_total_spend 
WHERE rank <= 2
ORDER BY category ASC, total_spend DESC