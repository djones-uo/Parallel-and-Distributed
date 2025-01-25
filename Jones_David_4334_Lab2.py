# Databricks notebook source
#David Jones

#Question 1 - In one databricks notebook command, create Spark code that counts the number of primes in a 
#given range. Start by first creating a Python list of all the numbers in the range 100..10,000. 
#Then use Spark commands to create a parallel RDD from this list. Using only Spark map, filter, 
#reduce and/or count, count the number of primes in this range in parallel. You may use 
#lambdas or standard Python functions in your maps/filters/reductions.

a = [x for x in range(100,10001)]
aRdd = sc.parallelize(a)
prime = aRdd.filter(lambda x: int(x) > 0 and (x == 2 or x == 3 or [x % i == 0 for i in range(2,int(x**0.5)+1)].count(True) == 0))
prime.count()

# COMMAND ----------

#Question 2 - In one databricks notebook command, create Spark code that works with temperature in the 
#following way. Start with creating 1000 random Fahrenheit temperatures between 0..100 
#degrees F. This should be done in a standard Python list. Normally, we would load this data 
#from 1000 different observations, but for this lab we will simply generate random test data. 
#Next use Spark RDDs (only single ones – no pairRDDs) and only the Spark commands map, filter, 
#reduce and/or count to first convert the full list to Celsius. Then find the average of all the 
#Celsius values above freezing. You should print that average. You are only to use lambdas in 
#your maps/filters/reductions. And you should persist RDDs if helps reduce computations.

import random
f = [random.randint(0,100) for i in range(1000)]
fRdd = sc.parallelize(f)
cRdd = fRdd.map(lambda x: (x-32)*5/9)
cRdd.filter(lambda x: x>0).mean()

# COMMAND ----------


