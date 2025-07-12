from pyspark.sql import SparkSession
# creating spark session
spark = SparkSession.builder.master("local[1]") \
 .appName('SparkByExamples.com') \
 .getOrCreate()