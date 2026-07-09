-- Credit Card Launch Month Performance
-- Difficulty: MEDIUM | Companies | Hints

-- ---- Problem ----
-- Your team at a leading financial institution is preparing to launch a new credit card. To
-- estimate how many cards will be issued in the first month, you need to analyze how
-- previous credit card launches performed during their debut months. Write a query that
-- retrieves the name of each credit card and the number of cards issued in its launch month.
--
-- Schema columns:
-- ccd_credit_cards: issue_month, issue_year, card_name, issued_amount
--
-- Output columns: card_name, issued_amount
--
-- Sort the results by issued_amount DESC.

-- ---- Examples ----
-- Example 1

-- Input:
-- ccd_credit_cards:
-- issue_month | issue_year | card_name     | issued_amount
-- 1           | 2021       | Sapphire Plus | 170000
-- 2           | 2021       | Sapphire Plus | 175000
-- 3           | 2021       | Sapphire Plus | 180000
-- 3           | 2021       | Freedom Flex  | 65000

-- Output:
-- card_name     | issued_amount
-- Sapphire Plus | 170000
-- Freedom Flex  | 65000

-- ---- Constraints ----
-- - Handle NULL values appropriately
-- - Return results matching the expected output schema and order

--- write SQL Query here
-- Table Name `ccd_credit_cards`

WITH 
    rank_issue_credit_cards AS (
SELECT 
    card_name,
    issued_amount,
    RANK() OVER (PARTITION BY card_name ORDER BY MAKE_DATE(issue_year, issue_month, 1) ASC) AS rank
FROM ccd_credit_cards
    )
SELECT 
    card_name,
    issued_amount
FROM rank_issue_credit_cards
WHERE rank = 1
ORDER BY issued_amount DESC