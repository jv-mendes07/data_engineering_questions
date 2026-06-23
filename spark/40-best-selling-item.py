## Best Selling item
## Difficulty: HARD | Companies | Hints

## ---- Problem ----
## Find the best selling item(s) by total quantity sold. Return item details and sales metrics.
##
## Schema columns: items.item_id, items.item_name, items.category,
## order_items.order_id, order_items.item_id, order_items.quantity,
## order_items.order_date
##
## Output columns: item_id, item_name, total_quantity_sold
##
## Sort results by total_quantity_sold DESC, item_id.

## ---- Examples ----
## Example 1

## Input:
## items:
## item_id | item_name | category
## 1       | Laptop    | Electronics
## 2       | Mouse     | Electronics
## 3       | Keyboard  | Electronics
## 4       | Monitor   | Electronics

## order_items:
## order_id | item_id | quantity | order_date
## 1        | 1       | 1        | 2023-01-15
## 2        | 2       | 3        | 2023-01-20
## 3        | 3       | 2        | 2023-02-10
## 4        | 1       | 2        | 2023-02-22

## Output:
## item_id | item_name | total_quantity_sold
## 2       | Mouse     | 14
## 1       | Laptop    | 7
## 3       | Keyboard  | 5
## 12      | USB Cable | 5

## Explanation: The output is derived by applying the required transformations to the input
## data as described in the problem statement.

## ---- Constraints ----
## - Multiple items with same total quantity
## - Items with no sales
## - NULL quantities


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(items, order_items):
    df_final = items.join(order_items, on = 'item_id', how = 'inner')

    df_final = df_final.groupBy('item_id', 'item_name').agg(
        F.sum(F.col('quantity')).alias('total_quantity_sold')
    ).orderBy(F.col('total_quantity_sold').desc())

    return df_final