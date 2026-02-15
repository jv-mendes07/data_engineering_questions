--You are provided with two tables:
--
--ad_update_advertiser Table: Contains information about advertisers and their current payment status.
--ad_update_daily_pay Table: Contains payment information for advertisers who have made payments on a specific day.
--Your task is to write a query that updates the payment status of advertisers based on their payment activity in the ad_update_daily_pay table. The query should determine the new status for each advertiser and output the user_id and the corresponding new_status, sorted by the user_id.
--
--Payment Status Categories:
--NEW: Advertisers who are newly registered and have made their first payment.
--EXISTING: Advertisers who have made payments in the past and have recently made a current payment.
--CHURN: Advertisers who have made payments in the past but have not made any recent payment.
--RESURRECT: Advertisers who were previously classified as "CHURN" but have recently made a payment.
--Status Transition Rules:
--Current Status	Payment Made	New Status
--NEW	Yes	EXISTING
--NEW	No	CHURN
--EXISTING	Yes	EXISTING
--EXISTING	No	CHURN
--CHURN	Yes	RESURRECT
--CHURN	No	CHURN
--RESURRECT	Yes	EXISTING
--RESURRECT	No	CHURN
-- 
--
--ad_update_advertiser 
--
--| user_id | status   |
--|---------|----------|
--| bing    | NEW      |
--| yahoo   | NEW      |
--| alibaba | EXISTING |
--
--ad_update_daily_pay
--
--| user_id | paid  |
--|---------|-------|
--| yahoo   | 45.00 |
--| alibaba | 100.00|
--| target  | 13.00 |
--
-- 
--
--Output
--
--| user_id   | new_status |
--|-----------|------------|
--| alibaba   | EXISTING   |
--| bing      | CHURN      |
--| yahoo     | EXISTING   |
--
---- Write query here
---- TABLE NAME: ad_update_advertiser & ad_update_daily_pay
--
SELECT 
  aua.user_id,
  CASE 
    WHEN aua.status = 'NEW' AND audp.paid IS NOT NULL THEN 'EXISTING'
    WHEN aua.status = 'NEW' AND audp.paid IS NULL THEN 'CHURN'
    WHEN aua.status = 'EXISTING' AND audp.paid IS NOT NULL THEN 'EXISTING'
    WHEN aua.status = 'EXISTING' AND audp.paid IS NULL THEN 'CHURN'
    WHEN aua.status = 'CHURN' AND audp.paid IS NOT NULL THEN 'RESURRECT'
    WHEN aua.status = 'CHURN' AND audp.paid IS NULL THEN 'CHURN'
    WHEN aua.status = 'RESURRECT' AND audp.paid IS NOT NULL THEN 'EXISTING'
    WHEN aua.status = 'RESURRECT' AND audp.paid IS NULL THEN 'CHURN'
    ELSE NULL
  END AS new_status
FROM ad_update_advertiser AS aua
LEFT JOIN ad_update_daily_pay AS audp 
ON audp.user_id = aua.user_id
ORDER BY aua.user_id