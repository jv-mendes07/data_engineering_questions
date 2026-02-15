#You are given two tables that track product listings and their order activity:
#
#Table 1: poa_products
#Contains metadata about each product.
#
#Column Name	Data Type
#product_id	int
#product_name	varchar
#product_category	varchar
#product_id is the primary key.
#
#Table 2: poa_orders
#Records when and how many units were ordered per product.
#
#Column Name	Data Type
#product_id	int
#order_date	date
#unit	int
#🎯 Task:
#Write a function or SQL query to return the names of products that had a total of at least 100 units ordered during February 2020, along with the total number of units ordered.
#
#📤 Output Requirements:
#Output columns:
#
#product_name	unit
#Sort the results in ascending order by the unit count.
#
#Only include products that reached 100 units or more in total during February 2020.
#
#Duplicate entries of the same product on different dates should be aggregated.
#
#🧪 Example Input:
#poa_products
#product_id	product_name	product_category
#1	Alpha Notebook	Electronics
#2	Beta Reference	Books
#3	Gamma PC	Electronics
#4	Delta Keyboard	Accessories
#5	Epsilon Tee	Apparel
#poa_orders
#product_id	order_date	unit
#1	2020-02-05	60
#1	2020-02-10	70
#2	2020-02-11	80
#3	2020-02-17	2
#3	2020-02-24	3
#4	2020-03-01	20
#5	2020-02-25	50
#5	2020-02-27	50
#✅ Expected Output:
#product_name	unit
#Epsilon Tee	100
#Alpha Notebook	130
#🧠 Explanation:
#Alpha Notebook: 60 + 70 = 130 units → ✅
#
#Epsilon Tee: 50 + 50 = 100 units → ✅
#
#Beta Reference: Only 80 units → ❌
#
#Gamma PC: Only 5 units → ❌
#
#Delta Keyboard: Not ordered in Feb 2020 → ❌
#
#📌 Note: Final result must be sorted by unit in ascending order.

import pandas as pd

def etl(poa_orders, poa_products):
    poa_orders['order_date'] = poa_orders.order_date.astype("datetime64[ns]")

    poa_orders_filtered = poa_orders[(poa_orders.order_date.dt.year == 2020) & (poa_orders.order_date.dt.month == 2)]

    poa_orders_products = pd.merge(poa_products, poa_orders_filtered, on='product_id')

    poa_orders_products = poa_orders_products.groupby(['product_name']).agg({'unit':'sum'}).reset_index()

    poa_orders_products = poa_orders_products[poa_orders_products['unit'] >= 100].sort_values('unit', ascending = True)

    return poa_orders_products
    
    