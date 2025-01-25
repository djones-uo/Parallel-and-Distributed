# Databricks notebook source
#Lab 3 for David Jones

#create new folder in DBFS Directory
#short Files
dbutils.fs.mkdirs("dbfs:/FileStore/tables/lab3Short")


short_files = ["shortLab3data0.txt", "shortLab3data1.txt"]

#move files to new DBFS locations
#short Files
for i in short_files:
    dbutils.fs.mv("dbfs:/FileStore/tables/"+i, "dbfs:/FileStore/tables/lab3Short", True)

# COMMAND ----------

#create new folder in DBFS Directory

#long Files
dbutils.fs.mkdirs("dbfs:/FileStore/tables/lab3Long")

long_files = ["fullLab3data0.txt", "fullLab3data1.txt", "fullLab3data2.txt", "fullLab3data3.txt"]

#long Files
for i in long_files:
    dbutils.fs.mv("dbfs:/FileStore/tables/"+i, "dbfs:/FileStore/tables/lab3Long", True)

# COMMAND ----------

#short files
sc=spark.sparkContext
 
#reads in file
file = sc.textFile('dbfs:///FileStore/tables/lab3Short/') 

#splits words in each line. Collect brings back list of strings
links = file.map(lambda line: line.split(" "))

def reflink(l):
    keys = l[1:]
    values = l[0]
    for i in keys:
        return (i,values)

#tuples of key/value of word and 1 for count later
tuples = links.map(lambda line: reflink(line))

gbk = tuples.groupByKey()
list_value = gbk.mapValues(list)
sbk=list_value.sortByKey()
sbk.collect()

# COMMAND ----------

#Full files
sc=spark.sparkContext
 
#reads in file
file = sc.textFile('dbfs:///FileStore/tables/lab3Long/') 

#splits words in each line. Collect brings back list of strings
linksf = file.map(lambda line: line.split(" "))

#tuples of key/value of word and 1 for count later
tuplesf = linksf.map(lambda line: reflink(line))

gbkf = tuplesf.groupByKey()
list_valuef = gbkf.mapValues(list)
sbkf =list_valuef.sortByKey()
sbkf.take(10)

# COMMAND ----------

sbkf.count()

# COMMAND ----------


