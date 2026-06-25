-- August 2022 Top Contributors
-- Difficulty: EASY | Companies | Hints

-- ---- Problem ----
-- Write a query to determine the top 2 contributors who sent the highest number of
-- messages within a messaging platform in August 2022. Display the user IDs of these
-- contributors along with the total count of messages they sent during this period. Ensure
-- the results are sorted in descending order based on the number of messages sent.
--
-- Schema columns:
-- dqt_messages: message_id, sender_id, receiver_id, content, sent_date
--
-- Output columns: sender_id, message_count

-- ---- Examples ----
-- Example 1

-- Input:
-- dqt_messages:
-- message_id | sender_id | receiver_id | content              | sent_date
-- 901        | 101       | 202         | Hello there!         | 2022-08-03 10:00:00
-- 902        | 202       | 101         | Hi, what's up?       | 2022-08-03 10:15:00
-- 903        | 101       | 303         | Meeting rescheduled. | 2022-08-05 14:30:00
-- 904        | 101       | 202         | Can we sync later?   | 2022-08-07 09:00:00

-- Output:
-- sender_id | message_count
-- 101       | 3
-- 202       | 1

-- ---- Constraints ----
-- - Handle NULL values appropriately
-- - Return results matching the expected output schema and order

-- Write query here
-- USE TABLE NAME `dqt_messages`

SELECT 
    sender_id,
    COUNT(DISTINCT message_id) AS message_count
FROM dqt_messages
WHERE EXTRACT(YEAR FROM sent_date) = 2022
AND EXTRACT(MONTH FROM sent_date) = 8
GROUP BY sender_id
ORDER BY message_count DESC
LIMIT 2