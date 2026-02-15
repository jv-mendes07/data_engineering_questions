--A country is considered large if it meets at least one of the following criteria:
--
--Its area is greater than or equal to 3,000,000 square kilometers.
--
--Its population is greater than or equal to 25,000,000 people.
--
--Your task is to write an SQL query to retrieve the name, population, and area of all such large countries.
--
--🛢️ Table: bne_countries_val
--This table contains data about various countries, including their geographical and economic details.
--
--Column Name	Data Type	Description
--name	varchar	Primary key; unique country name
--continent	varchar	Continent where the country is located
--area	int	Total land area in square kilometers
--population	int	Total population of the country
--gdp	bigint	Gross Domestic Product
--📥 Sample Input
--name	continent	area	population	gdp
--Afghanistan	Asia	652230	25500100	20343000000
--Albania	Europe	28748	2831741	12960000000
--Algeria	Africa	2381741	37100000	188681000000
--Andorra	Europe	468	78115	3712000000
--Angola	Africa	1246700	20609294	100990000000
--📤 Expected Output
--name	population	area
--Afghanistan	25500100	652230
--Algeria	37100000	2381741
--🔍 Explanation
--Afghanistan qualifies due to population ≥ 25,000,000.
--
--Algeria qualifies due to population ≥ 25,000,000.
--
--Other countries do not meet either threshold.

-- Write query here
-- TABLE NAME: `bne_countries_val`

SELECT 
  name,
  population,
  area
FROM bne_countries_val 
WHERE 
  (area >= 3000000)
OR (population >= 25000000)