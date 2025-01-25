# Databricks notebook source
#Lab 7 for David Jones

#create new folder in DBFS Directory
dbutils.fs.mkdirs("dbfs:/FileStore/tables/lab7")


#move files to new DBFS locations
dbutils.fs.mv("dbfs:/FileStore/tables/heartTraining.csv", "dbfs:/FileStore/tables/lab7", True)
dbutils.fs.mv("dbfs:/FileStore/tables/heartTesting.csv", "dbfs:/FileStore/tables/lab7", True)

# COMMAND ----------

#load the data into a dataframe
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, IntegerType

dataSchema = StructType([StructField('id', LongType(), True),\
                    StructField('age', LongType(), True),\
                    StructField('sex', StringType(), True),\
                    StructField('chol', LongType(), True),\
                    StructField('pred', StringType(), True)])

testData = spark.read.format('csv').option('header', True).schema(dataSchema).load("dbfs:/FileStore/tables/lab7/heartTesting.csv")
trainData = spark.read.format('csv').option('header', True).schema(dataSchema).load("dbfs:/FileStore/tables/lab7/heartTraining.csv")

# COMMAND ----------

#verify training data imported correctly
trainData.show()

# COMMAND ----------

#verify testing data imported correctly
testData.show()

# COMMAND ----------

#pipeline building - Categorical Variables
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler, Bucketizer, StringIndexer

sexNum = StringIndexer(inputCol='sex', outputCol='SexValue')
predNum = StringIndexer(inputCol='pred', outputCol='label')

# COMMAND ----------

#pipeline building - age

ageBuckets = Bucketizer(splits=[0,40,50,60,70,200], inputCol='age', outputCol='AgeCats')

# COMMAND ----------

#pipeline building - Vectors
vecs = VectorAssembler(inputCols=['chol', 'AgeCats', 'SexValue'], outputCol='features')

# COMMAND ----------

#pipeline building - model building and final pipeline
lr = LogisticRegression()
lr_pipe = Pipeline(stages = [ageBuckets, sexNum, predNum, vecs, lr])
lr_model = lr_pipe.fit(trainData)

# COMMAND ----------

#Testing model on test data
test_pred = lr_model.transform(testData)
test_pred.select('id', 'probability', 'prediction').show()

# COMMAND ----------

#Testing model on train data
train_pred = lr_model.transform(trainData)
train_pred.select('id', 'age', 'sex', 'chol', 'probability', 'label', 'prediction').show()

# COMMAND ----------


