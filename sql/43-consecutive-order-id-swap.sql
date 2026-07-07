-- Consecutive Order ID Swap
-- Difficulty: MEDIUM | Companies | Hints

-- ---- Problem ----
-- Imagine a leading food delivery platform that connects customers with restaurants and
-- cuisines, enabling them to browse menus, place orders, and receive meals at their
-- doorstep. The platform recently encountered a glitch in its delivery instructions, causing
-- the food items to be mismatched with the wrong orders. Each item's order was swapped
-- with the item in the next row.
--
-- Schema columns:
-- osf_order_swap: order_id, item
--
-- Output columns: corrected_order_id, item
--
-- Sort the results by corrected_order_id.

-- ---- Examples ----
-- Example 1

-- Input:
-- osf_order_swap:
-- order_id | item
-- 1        | Chow Mein
-- 2        | Pizza
-- 3        | Pad Thai
-- 4        | Butter Chicken

-- Output:
-- corrected_order_id | item
-- 1                  | Pizza
-- 2                  | Chow Mein
-- 3                  | Butter Chicken
-- 4                  | Pad Thai

-- ---- Constraints ----
-- - Handle NULL values appropriately
-- - Return results matching the expected output schema and order

-- Write query here
-- TABLE NAME: `osf_order_swap`

SELECT 
    CASE 
        WHEN order_id % 2 = 0 AND order_id != (SELECT MAX(order_id) FROM osf_order_swap) THEN order_id - 1
        WHEN order_id % 2 != 0 AND order_id != (SELECT MAX(order_id) FROM osf_order_swap) THEN order_id + 1
        ELSE order_id
    END AS corrected_order_id,
    item
FROM osf_order_swap
ORDER BY corrected_order_id ASC;