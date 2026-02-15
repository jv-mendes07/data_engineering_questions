#You are given a table or DataFrame named tsr_songs that stores information about songs, their rankings, and their artists.
#
#Your task is to find the top 10 songs from the year 2010, based on their year_rank. Ensure that:
#
#Each song appears only once (no duplicates).
#
#The output includes:
#
#Rank (year_rank)
#
#Group Name
#
#Song Name
#
#The results should be sorted by ranking in ascending order.
#
#📦 Table / DataFrame: tsr_songs
#Column Name	Type	Description
#year	INTEGER	Year the song was ranked
#year_rank	INTEGER	Rank of the song for that year
#group_name	TEXT	Performing group or band name
#artist	TEXT	Main artist (can match group name)
#song_name	TEXT	Name of the song
#id	INTEGER	Unique ID for the song entry
#📋 Sample Input (tsr_songs)
#year	year_rank	group_name	artist	song_name	id
#2010	1	Lady Antebellum	Lady Antebellum	Need You Now	101
#2010	2	Train	Train	Hey, Soul Sister	102
#2010	3	Katy Perry	Katy Perry	California Gurls	103
#2010	4	Usher	Usher	OMG	104
#2010	5	B.o.B	B.o.B	Airplanes	105
#2010	6	Eminem	Eminem	Love The Way You Lie	106
#2010	7	Taio Cruz	Taio Cruz	Dynamite	107
#2010	8	Bruno Mars	Bruno Mars	Just The Way You Are	108
#2010	9	Ke$ha	Ke$ha	TiK ToK	109
#2010	10	Black Eyed Peas	Black Eyed Peas	I Gotta Feeling	110
#2010	10	Black Eyed Peas	Black Eyed Peas	I Gotta Feeling (Remix)	111
#✅ Expected Output
#Rank	Group Name	Song Name
#1	Lady Antebellum	Need You Now
#2	Train	Hey, Soul Sister
#3	Katy Perry	California Gurls
#4	Usher	OMG
#5	B.o.B	Airplanes
#6	Eminem	Love The Way You Lie
#7	Taio Cruz	Dynamite
#8	Bruno Mars	Just The Way You Are
#9	Ke$ha	TiK ToK
#10	Black Eyed Peas	I Gotta Feeling
#💡 Notes:
#In case of duplicate songs with the same year_rank, pick only one (preferably the first unique song_name).
#
#Ensure that the output contains exactly 10 unique songs.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def etl(tsr_songs):

  tsr_songs = tsr_songs.dropDuplicates(subset=['song_name', 'id'])
  
  tsr_songs = tsr_songs.filter(
    F.col('year') == '2010'
  ).select(
    F.col('year_rank').alias('Rank'),
    F.col('group_name').alias('Group Name'),
    F.col('song_name').alias('Song Name')
  )

  tsr_songs = tsr_songs.orderBy(F.col('year_rank').asc()).limit(10)

  return tsr_songs