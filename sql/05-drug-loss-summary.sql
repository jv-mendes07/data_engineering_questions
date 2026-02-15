--A leading pharmacy chain, CVS Health, wants to analyze its financial performance by identifying losses incurred on drugs, each of which is exclusively produced by a specific manufacturer.
--
--🧠 Objective
--Write a SQL query to:
--
--Identify manufacturers whose drugs have caused a financial loss (i.e., COGS > total_sales).
--
--For each such manufacturer, calculate:
--
--The number of loss-making drugs (drug_count)
--
--The total monetary loss (total_loss) as an absolute value
--
--Sort the result in descending order of total_loss, with the manufacturer having the highest loss listed first.
--
--🛢️ Table: dls_drug_loss
--Column Name	Data Type	Description
--product_id	integer	Unique identifier for the product
--units_sold	integer	Number of units sold
--total_sales	decimal	Total revenue generated from the product
--cogs	decimal	Cost of Goods Sold for the product
--manufacturer	varchar	Manufacturer of the drug
--drug	varchar	Name of the drug
--📥 Sample Input
--product_id	units_sold	total_sales	cogs	manufacturer	drug
--156	89514	3130097.00	3427421.73	Biogen	Acyclovir
--25	222331	2753546.00	2974975.36	AbbVie	Lamivudine and Zidovudine
--50	90484	2521023.73	2742445.90	Eli Lilly	Dermasorb TA Complete Kit
--98	110746	813188.82	140422.87	Biogen	Medi-Chord
--📤 Expected Output
--manufacturer	drug_count	total_loss
--Biogen	1	297324.73
--AbbVie	1	221429.36
--Eli Lilly	1	221422.17
--🔍 Explanation
--A loss is recorded when cogs > total_sales.
--
--For example:
--
--Biogen's drug Acyclovir caused a loss of 3427421.73 - 3130097.00 = 297324.73.
--
--AbbVie's drug Lamivudine and Zidovudine had a loss of 2974975.36 - 2753546.00 = 221429.36.
--
--Profitable drugs, like Medi-Chord from Biogen (where cogs < total_sales), are excluded.
--
--Results are ordered by the largest total loss first.
--
--✅ Output Requirements
--Return a result with the following columns:
--
--manufacturer: Name of the manufacturer
--
--drug_count: Number of loss-making drugs
--
--total_loss: Sum of all losses for that manufacturer (as absolute values)

-- Write query here
-- TABLE NAME: `dls_drug_loss`
SELECT 
  manufacturer,
  COUNT(product_id) AS drug_count,
  ROUND(SUM(cogs::numeric) - SUM(total_sales::numeric), 2) AS total_loss
FROM dls_drug_loss
WHERE cogs > total_sales
GROUP BY manufacturer
ORDER BY total_loss DESC