/*
============================================================
 Most Frequent Order Type (MEDIUM)
============================================================

Problem:
  You are provided with a table om_items_per_order containing
  the count of items per order (item_count) and the frequency
  of orders with that same item count (order_occurrences).

  Write an SQL query to determine the mode(s) of the order
  occurrences, i.e., the item_count value(s) with the highest
  order_occurrences. If multiple item counts share the same
  highest frequency, return all of them.

Schema:
  om_items_per_order (2 columns)
    - item_count         : number of items in the order
    - order_occurrences  : frequency of orders with that item count

Output columns:
  - mode

Sort order:
  - Results sorted by item_count (ascending)

Example 1:
  Input (om_items_per_order):
    item_count | order_occurrences
    1          | 500
    2          | 1000
    3          | 800
    4          | 1000

  Output:
    mode
    2
    4

Constraints:
  - Handle NULL values appropriately
  - Return results matching the expected output schema and order
============================================================
*/

-- Write query here
-- TABLE NAME: `om_items_per_order`

SELECT 
    item_count AS mode 
FROM om_items_per_order AS oipo 
WHERE order_occurrences = (SELECT MAX(order_occurrences) FROM om_items_per_order)