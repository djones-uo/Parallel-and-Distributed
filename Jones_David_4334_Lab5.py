# Databricks notebook source
#Lab 5 for David Jones

#create new folder in DBFS Directory
#short Files
dbutils.fs.mkdirs("dbfs:/FileStore/tables/lab5")


#move files to new DBFS locations
#short Files
dbutils.fs.mv("dbfs:/FileStore/tables/Master.csv", "dbfs:/FileStore/tables/lab5", True)

# COMMAND ----------

sc = spark.sparkContext
#initialize file and map the RDD
file = "dbfs:/FileStore/tables/lab5/Master.csv"
mlbRDD = sc.textFile(file).map(lambda l: l.split(","))
# skip header
header = mlbRDD.first()
mlbRDD = mlbRDD.filter(lambda x: x!=header)
mlbRDD = mlbRDD.filter(lambda x: x[17] != '')
mlbRDD.collect()

# COMMAND ----------

from pyspark.sql import Row
mlbRowsRDD = mlbRDD.map(lambda l: Row(playerID=l[0], birthCountry=l[4], birthState=l[5], height=int(l[17])))
mlbRowsRDD.collect()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType

mlbSchema = StructType([\
				StructField('playerID', StringType(), True), \
				StructField('birthCountry', StringType(), True), \
				StructField('birthState', StringType(), True), \
				StructField('height', StringType(), True), \
				])

# COMMAND ----------

mlbDF = spark.createDataFrame(mlbRowsRDD, mlbSchema)

# COMMAND ----------

mlbDF.show(30)

# COMMAND ----------

mlbDF.createOrReplaceTempView('mlb')

# COMMAND ----------

#Find the number of players who were born in the state of Colorado:
#SQL

spark.sql("SELECT count(*) from mlb WHERE birthState == 'CO'").show()

# COMMAND ----------

#Find the number of players who were born in the state of Colorado:
#DataFrame
import pyspark.sql.functions as f

mlbDF.filter(f.col('birthState')=='CO')\
		.select(f.count('birthState')).show()

# COMMAND ----------

#List the average height by birth country of all players, ordered from highest to lowest.
#SQL

# spark.sql("SELECT AVG(height), birthCountry from mlb ORDER BY height").show()
spark.sql("SELECT birthCountry, AVG(height) AS AverageHeight from mlb GROUP BY birthCountry ORDER BY AverageHeight DESC").show()

# COMMAND ----------

#List the average height by birth country of all players, ordered from highest to lowest.
#DataFrame

mlbDF.select('height', 'birthCountry').groupby('birthCountry').agg(f.avg('height').alias("AverageHeight")).sort(f.desc("AverageHeight")).show()

# COMMAND ----------


