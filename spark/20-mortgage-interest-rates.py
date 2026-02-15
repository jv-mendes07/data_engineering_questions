#You are working for a mortgage company that manages various mortgage types selected by multiple users. The company stores this data in two DataFrames:
#
#MortgageDetails: Contains details of each mortgage type
#
#UserMortgages: Tracks which users are associated with which mortgage
#
#Each mortgage is uniquely identified by a MortgageID, and each user by a UserID.
#
#🧾 Task
#Write a function etl(MortgageDetails, UserMortgages) that returns a DataFrame containing the rate of mortgage for each MortgageType.
#
#📘 Input Schemas
#MortgageDetails
#Column Name	Type
#MortgageID	String
#MortgageType	String
#InterestRate	Double
#UserMortgages
#Column Name	Type
#UserID	String
#MortgageID	String
#📤 Output Schema
#Column Name	Type
#MortgageType	String
#RateOfMortgage	Double
#📌 Definition of RateOfMortgage:
#For each MortgageType, calculate:
#sum of all InterestRates associated with that type
#divided by the number of distinct UserIDs who selected it.
#
#🧪 Example
#Input – MortgageDetails
#MortgageID	MortgageType	InterestRate
#M1	Fixed	4.5
#M2	Variable	3.2
#M3	Adjustable	2.8
#Input – UserMortgages
#UserID	MortgageID
#U1	M1
#U2	M1
#U3	M2
#U4	M3
#Expected Output
#MortgageType	RateOfMortgage
#Adjustable	2.8
#Fixed	4.5
#Variable	3.2


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(MortgageDetails, UserMortgages):
  
  MortgageRate = MortgageDetails.join(UserMortgages, how = 'inner', on = 'MortgageID')

  MortgageRate = (MortgageRate
                  .groupBy('MortgageType')
                  .agg(
                    F.sum('InterestRate').alias('InterestRate'),
                    F.countDistinct('UserID').alias('countUsers')
                  ))

  MortgageRate = (MortgageRate
                  .withColumn(
        'RateOfMortgage',
        F.round(F.col('InterestRate') / F.col('countUsers'), 2)
        )
                  .select(
                    F.col('MortgageType'),
                    F.col('RateOfMortgage')
                  ))
  
  return MortgageRate