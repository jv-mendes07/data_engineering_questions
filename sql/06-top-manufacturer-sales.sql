--CVS Health aims to analyze its pharmacy sales data to better understand the overall product performance across manufacturers.
--
--🧠 Objective
--Write a SQL query to:
--
--Calculate the total sales of all drugs produced by each manufacturer.
--
--Round the total sales to the nearest million.
--
--Format the result as a string in the form: "$36 million".
--
--Display the results:
--
--In descending order of total sales.
--
--If two manufacturers have the same total sales, sort them alphabetically by manufacturer name.
--
--🛢️ Table: tms_pharma_in
--Column Name	Data Type	Description
--product_id	integer	Unique identifier for the product
--units_sold	integer	Number of units sold
--total_sales	decimal	Total revenue generated from the product
--cogs	decimal	Cost of Goods Sold
--manufacturer	varchar	Manufacturer of the drug
--drug	varchar	Name of the drug
--📥 Sample Input
--product_id	units_sold	total_sales	cogs	manufacturer	drug
--94	132362	2041758.41	1373721.70	Biogen	UP and UP
--9	37410	293452.54	208876.01	Eli Lilly	Zyprexa
--50	90484	2521023.73	2742445.90	Eli Lilly	Dermasorb
--61	77023	500101.61	419174.97	Biogen	Varicose Relief
--136	144814	1084258.00	1006447.73	Biogen	Burkhart
--📤 Expected Output
--manufacturer	sale
--Biogen	$4 million
--Eli Lilly	$3 million
--🔍 Explanation
--First, total sales are summed per manufacturer:
--
--Biogen = 2,041,758.41 + 500,101.61 + 1,084,258.00 = 3,626,118.02
--
--Eli Lilly = 293,452.54 + 2,521,023.73 = 2,814,476.27
--
--Then, total sales are rounded to the nearest million:
--
--Biogen → $4 million
--
--Eli Lilly → $3 million
--
--Final results are:
--
--Sorted by highest sales first
--
--With alphabetical tiebreaker for equal sales
--
--✅ Output Format Requirements
--Return a result with the following columns:
--
--manufacturer: Name of the manufacturer
--
--sale: Total sales (rounded and formatted as a string like "$3 million")

-- Write query here
-- TABLE NAME: `tms_pharma_in`

SELECT 
  manufacturer,
  '$'||(CEIL(SUM(total_sales) / 1000000))::integer||' million' AS sale
FROM tms_pharma_in
GROUP BY 
  manufacturer
ORDER BY sale DESC