# Databricks notebook source
#Lab 6 for David Jones

#move files to same directory as 'Master.csv' file
dbutils.fs.mv("dbfs:/FileStore/tables/AllstarFull.csv", "dbfs:/FileStore/tables/lab5", True)
dbutils.fs.mv("dbfs:/FileStore/tables/Teams.csv", "dbfs:/FileStore/tables/lab5", True)

# COMMAND ----------

sc = spark.sparkContext

master = spark.read.csv("dbfs:/FileStore/tables/lab5/Master.csv", inferSchema=True, header=True)
allstar = spark.read.csv("dbfs:/FileStore/tables/lab5/AllstarFull.csv", inferSchema=True, header=True)
teams = spark.read.csv("dbfs:/FileStore/tables/lab5/Teams.csv", inferSchema=True, header=True)


# COMMAND ----------

#join the threee dataframes on IDs and select necessary columns
master_as_team = allstar.join(teams, 'teamID', "left_outer").join(master, 'playerID', "left_outer")

mlb = master_as_team.select("playerID", "teamID", "nameFirst", "nameLast", "name")
mlb.show(truncate=False)

# COMMAND ----------

#write parquet file

mlbparquet = "dbfs:///FileStore/tables/lab6"

#remove existing file
dbutils.fs.rm(mlbparquet, True)

mlb.write.format('parquet').mode('overwrite').save(mlbparquet)

# COMMAND ----------

#read in parquet file
mlb2 = spark.read.format('parquet').load(mlbparquet)

mlb3 = mlb2.distinct()
mlb3.show()

# COMMAND ----------

#give the first and last names of all the Colorado Rockies allstars. Display both the number of all stars as well as showing the full list of them.
import pyspark.sql.functions as f

mlb3.filter(f.col('name')=='Colorado Rockies')\
		.select(f.count('name')).show()

# COMMAND ----------

mlb3.filter(f.col('name')=='Colorado Rockies')\
		.select('nameLast', 'nameFirst').sort('nameLast').show(24)

# COMMAND ----------


