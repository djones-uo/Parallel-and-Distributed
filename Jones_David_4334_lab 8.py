# Databricks notebook source
#Lab 8 for David Jones

#create new folder in DBFS Directory
dbutils.fs.mkdirs("dbfs:/FileStore/tables/lab8")


#move files to new DBFS locations
dbutils.fs.mv("dbfs:/FileStore/tables/FIFA.csv", "dbfs:/FileStore/tables/lab8", True)

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType

fifaSchema = StructType( \
                        [StructField('ID', LongType(), True), \
                         StructField('lang', StringType(), True), \
                         StructField('Date', TimestampType(), True), \
                         StructField('Source', StringType(), True), \
                         StructField('len', LongType(), True), \
                         StructField('Orig_Tweet', StringType(), True), \
                         StructField('Tweet', StringType(), True), \
                         StructField('Likes', LongType(), True), \
                         StructField('RTs', LongType(), True), \
                         StructField('Hashtags', StringType(), True), \
                         StructField('UserMentionNames', StringType(), True), \
                         StructField('UserMentionID', StringType(), True), \
                         StructField('Name', StringType(), True), \
                         StructField('Place', StringType(), True), \
                         StructField('Followers', LongType(), True), \
                         StructField('Friends', LongType(), True), \
                        ])

fifa = spark.read.format('csv').option('header', True).schema(fifaSchema).option('ignoreLeadingWhiteSpace', True).option('mode', 'dropMalformed').load("dbfs:/FileStore/tables/lab8/FIFA.csv")

fifa.show()

# COMMAND ----------

#drop all columns besides ID, Date and Hashtag
fifa_cols = fifa.select("ID", "Date", "Hashtags")
#drop null rows
fifa_values = fifa_cols.filter(f.col('Hashtags').isNotNull())
#explode Hashtags column
static_fifa = fifa_values.withColumn('Hashtags', f.explode(f.split('Hashtags', ',')))
static_fifa.show()

# COMMAND ----------

#implement the window on the static fifa dataframe
final_fifa = static_fifa.groupBy(f.window("Date", "60 minutes", "30 minutes"), "Hashtags").agg(f.count("Hashtags").alias('HashCounts')).filter(f.col("HashCounts") > 100)


final_fifa.orderBy(f.col("window"),f.col("HashCounts")).show(20)

# COMMAND ----------

#partition the static_fifa into small files to help with streaming
part_fifa = static_fifa.repartition(10).persist()

# COMMAND ----------

part_fifa.rdd.glom().collect()

# COMMAND ----------

#remove existinf files
dbutils.fs.rm("FileStore/tables/fifa/", True)

#write files to pull in for streaming
part_fifa.write.format("parquet").option("header", True).save("FileStore/tables/fifa/")

# COMMAND ----------

#set up the stream
fifaStream = spark.readStream.format("parquet").option("header", True).schema(fifaSchema).option("maxFilesPerTrigger", 1).load("dbfs:///FileStore/tables/fifa")

fifaWin = fifaStream.withWatermark("Date", "24 hours").groupBy(f.window("Date", "60 minutes", "30 minutes"), "Hashtags").agg(f.count("Hashtags").alias('HashCounts')).filter(f.col("HashCounts") > 100)

# COMMAND ----------

sinkStream = fifaWin.writeStream.outputMode("complete").format("memory").queryName("fifaStr").start()

# COMMAND ----------

spark.sql("SELECT window, Hashtags, HashCounts FROM fifaStr ORDER BY fifaStr.window, fifaStr.HashCounts").show()

# COMMAND ----------


