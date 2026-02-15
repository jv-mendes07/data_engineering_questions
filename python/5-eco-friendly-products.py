#Write a query to find the product_id of products that are both low-fat and recyclable.
#
#A product is considered low-fat if the low_fats column is 'Y'.
#A product is considered recyclable if the recyclable column is 'Y'.
#Return the result as a table with a single column, product_id.
#The result can be returned in any order.
#
#Table: Products
#| Column Name  | Data Type | Description                                              |
#| ------------ | --------- | -------------------------------------------------------- |
#| `product_id` | `int`     | Unique identifier for each product.                      |
#| `low_fats`   | `enum`    | Indicates if the product is low fat (`'Y'` or `'N'`).    |
#| `recyclable` | `enum`    | Indicates if the product is recyclable (`'Y'` or `'N'`). |
#Sample Data:
#
#| product_id | low_fats | recyclable |
#| ---------- | -------- | ---------- |
#| 0          | Y        | N          |
#| 1          | Y        | Y          |
#| 2          | N        | Y          |
#| 3          | Y        | Y          |
#| 4          | N        | N          |
#Expected Output:
#| product_id |
#| ---------- |
#| 1          |
#| 3          |

import pandas as pd

def etl(products_df):
    products_df = products_df[(products_df['low_fats'] == 'Y') & (products_df['recyclable'] == 'Y')]

    products_df = products_df[['product_id']]

    return products_df