"""
============================================================
 Streamer Sessions After First Viewer Session (MEDIUM)
============================================================

Problem:
  Find users whose earliest Twitch session was as a viewer,
  then count all streamer sessions recorded for those users.
  Return user_id and streamer_session_count, with larger counts
  first; users tied on that count may appear in either order.
  Users whose first session was not a viewer are excluded.

Schema:
  sessions (4 columns)
    - user_id       : id of the user
    - session_id    : id of the session
    - session_date  : date the session occurred
    - session_type  : type of session ('viewer' or 'streamer')

Output columns:
  - user_id
  - streamer_session_count

Sort order:
  - Results sorted by streamer_session_count DESC
  - Ties may appear in either order

Example 1:
  Input (sessions):
    user_id | session_id | session_date | session_type
    101     | 1          | 2024-01-15   | viewer
    101     | 2          | 2024-01-20   | viewer
    101     | 3          | 2024-01-25   | streamer
    101     | 4          | 2024-02-01   | streamer
    102     | 5          | 2024-01-16   | viewer
    102     | 6          | 2024-01-21   | viewer
    102     | 7          | 2024-01-28   | viewer
    103     | 8          | 2024-01-17   | viewer

  Output:
    user_id | streamer_session_count
    101     | 2

  Explanation:
    User 101 begins with a viewer session and later has two
    streamer sessions, so the output count is 2.

Constraints:
  - Use only the records shown in the supplied tables.
  - Counts and calculations follow the business rules stated
    above.
  - Round numeric results to the precision shown in the output.
  - Session dates are unique within each user.
  - Return results matching the expected output schema and order.
============================================================
"""

user_id	session_id	session_date	session_type
101	1	2024-01-15	viewer
101	2	2024-01-20	viewer
101	3	2024-01-25	streamer
101	4	2024-02-01	streamer
102	5	2024-01-16	viewer
102	6	2024-01-21	viewer
102	7	2024-01-28	viewer
103	8	2024-01-17	viewer
Output:

user_id	streamer_session_count
101	2
Explanation: User 101 begins with a viewer session and later has two streamer sessions, so the output count is 2.

Constraints
Use only the records shown in the supplied tables.
Counts and calculations follow the business rules stated above.
Round numeric results to the precision shown in the output.
Session dates are unique within each user.
Return results matching the expected output schema and order.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(sessions):

    window = Window.partitionBy('user_id').orderBy('session_date')
    
    sessions = (sessions
                .withColumn(
        'first_type_session',
        F.first('session_type').over(window)))

    sessions = sessions.filter((F.col('first_type_session') == 'viewer') & (F.col('session_type') != 'viewer'))

    sessions = (sessions.groupBy('user_id')
                .agg(F.count('session_type').alias('streamer_session_count'))
.orderBy(F.col('streamer_session_count').desc()))

    return sessions