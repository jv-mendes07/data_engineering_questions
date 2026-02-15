--Meta is analyzing energy consumption patterns across its global data centers.
--You're provided with daily energy usage logs for Europe, Asia, and North America.
--
--👉 Your Task:
--Write a SQL query to identify the date with the highest total combined energy consumption across all three regions.
--
--For each date, sum the energy consumed across pec_eu_energy, pec_asia_energy, and pec_na_energy.
--
--Return the date with the highest total consumption and the total consumption value.
--
--If multiple dates have the same highest total, return the earliest date.
--
--🧾 Input Tables
--pec_eu_energy
--date	consumption
--2020-01-01	400
--2020-01-02	350
--2020-01-03	500
--2020-01-04	500
--2020-01-07	600
--pec_asia_energy
--date	consumption
--2020-01-01	400
--2020-01-02	400
--2020-01-04	675
--2020-01-05	1200
--2020-01-06	750
--2020-01-07	400
--pec_na_energy
--date	consumption
--2020-01-01	250
--2020-01-02	375
--2020-01-03	600
--2020-01-06	500
--2020-01-07	250
--✅ Expected Output
--date	total_consumption
--2020-01-07	1250
--💡 Notes
--Not all tables contain records for every date.
--
--Treat missing data as zero consumption for that region on that day.
--
--Use appropriate joins or unions to aggregate total consumption per date.
--
--Return only one row with the maximum total consumption, resolving ties by earliest date.

 

-- Write query here
-- TABLE NAME: `pec_asia_energy` or `pec_eu_energy` or `pec_na_energy`

WITH 
  energy_consumption_countries AS
(
  SELECT 
    date,
    consumption
  FROM pec_asia_energy
  UNION ALL 
  SELECT 
    date,
    consumption
  FROM pec_eu_energy
  UNION ALL
  SELECT 
    date,
    consumption
  FROM pec_na_energy
)
SELECT 
  date, 
  SUM(consumption) AS total_consumption
FROM energy_consumption_countries
GROUP BY 
  date
ORDER BY total_consumption DESC 
LIMIT 1