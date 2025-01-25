# Databricks notebook source
# #Assignment 2 for David Jones

# #create new folder in DBFS Directory
# dbutils.fs.mkdirs("dbfs:/FileStore/tables/assignment2")


# #move files to new DBFS locations
# dbutils.fs.mv("dbfs:/FileStore/tables/Affairs.csv", "dbfs:/FileStore/tables/assignment2", True)

# COMMAND ----------

#load the data into a dataframe
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, IntegerType, FloatType

affairSchema = StructType([StructField('id', LongType(), True),\
                    StructField('affairs', LongType(), True),\
                    StructField('gender', StringType(), True),\
                    StructField('age', FloatType(), True),\
                    StructField('yearsmarried', FloatType(), True),\
                    StructField('children', StringType(), True),\
                    StructField('religiousness', LongType(), True),\
                    StructField('education', LongType(), True),\
                    StructField('occupation', LongType(), True),\
                    StructField('rating', LongType(), True),\
                          ])

affairs = spark.read.format('csv').option('header', True).schema(affairSchema).load("dbfs:/FileStore/tables/assignment2/Affairs.csv")

# COMMAND ----------

#sort the id column so the affairs column isn't separated for splitting purposes later
affairs = affairs.sort('id')
affairs.show()

# COMMAND ----------

import pyspark.sql.functions as f

weights = [0.67, 0.33]

# Splitting the DataFrame
split_affairs = affairs.randomSplit(weights, seed=42)

# assign the training data
train = split_affairs[0]

# Split DataFrame 2
test = split_affairs[1]

# COMMAND ----------

#set up testing datat to stream in later
#repartition the test data
test_file = test.repartition(15).persist()
#remove folder if it exists
dbutils.fs.rm("dbfs:/FileStore/tables/assignment2/affairstest", True)
#write testing files to test folder
test_file.write.format("csv").option("header", True).save("dbfs:/FileStore/tables/assignment2/affairstest")

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler, Bucketizer, StringIndexer

#encode string variables
genderNum = StringIndexer(inputCol='gender', outputCol='gender_coded')
childrenNum = StringIndexer(inputCol='children', outputCol='children_coded')

#bucketize the output so it's 1 or 0
affairsBuckets =Bucketizer(splits=[0,1,100000], inputCol='affairs', outputCol='label')

# build vector
v = VectorAssembler(inputCols=['age','yearsmarried','religiousness','education','occupation','rating', 'gender_coded','children_coded'], outputCol='features')

#set up pipeline and estimator
lr = LogisticRegression()
steps = [genderNum, childrenNum, affairsBuckets, v, lr]
p = Pipeline(stages=steps)

# fit training data 
lrModel = p.fit(train)

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
# test model on training data
predict = lrModel.transform(train)
 
# evaluate prediction on training data
bc = BinaryClassificationEvaluator()
model_auc = bc.evaluate(predict)

mc = MulticlassClassificationEvaluator(metricName="accuracy")
model_acc = mc.evaluate(predict)

print(f"The accuracy of the model is {model_acc} and the AUC of the model is {model_auc}")

# COMMAND ----------

# #set up the stream
# affairStream = spark.readStream.format("csv").option("header", True).schema(affairSchema).option("maxFilesPerTrigger", 1).load("dbfs:/FileStore/tables/assignment2/affairstest")

# affairWin = affairStream.groupby(f.window("id", "30 seconds"))

# COMMAND ----------

affairReadStream = spark.readStream.format("csv").option("header", True).schema(affairSchema).option("maxFilesPerTrigger", 1).load("dbfs:/FileStore/tables/assignment2/affairstest")
affairPredictions = lrModel.transform(affairReadStream)
affairStream = affairPredictions.writeStream.outputMode("append").format("memory").queryName("affairTest").start()

# COMMAND ----------

#verify 
spark.sql("SELECT COUNT(affairTest.id) FROM affairTest").show()

# COMMAND ----------

test_predictions = spark.sql("SELECT * FROM affairTest")

# COMMAND ----------

# evaluate prediction on training data
bc = BinaryClassificationEvaluator()
test_auc = bc.evaluate(test_predictions)

mc = MulticlassClassificationEvaluator(metricName="accuracy")
test_acc = mc.evaluate(test_predictions)

print(f"The accuracy of the model is {test_acc} and the AUC of the model is {test_auc}")

# COMMAND ----------


