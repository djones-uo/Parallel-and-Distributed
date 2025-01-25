# Databricks notebook source
sc=spark.sparkContext
#initialize test list and parallelize it 
test = ["a b c", "b a a", "c b", "d a"]

test_p = sc.parallelize(test)
#deine mapping function
def pairmakerComp(urlline):
    urls = urlline.split(" ")
    return (urls[0], urls[1:])
#map functions on the test data to build the links RDD and collect to verify it was correct
urls = test_p.map(pairmakerComp)
urls.collect()

# COMMAND ----------

#Build the rank RDD

#pull distinct keys and parllelize
ranklink = urls.keys().distinct().collect()
test_rank = sc.parallelize(ranklink)

#map rank value to each key via initial distribution and collect to veriy it was correct
current_rank = test_rank.map(lambda x: (x, 1/len(ranklink)))
current_rank.collect()

# COMMAND ----------

#define function to match keys with ranks

def updateRank(l):
    rank_list = []
    if len(l[1][0]) == 1:
        rank = l[1][1]/len(l[1][0])
        link = l[1][0][0]
        pair = (link, rank)
        rank_list.append(pair)
    else:
        for i in range(len(l[1][0])):
            rank = l[1][1]/len(l[1][0])
            link = l[1][0][i]
            pair = (link, rank)
            rank_list.append(pair)
    return rank_list

#remove duplicate 'link-to' links via mapping values to a set
rem_dup = urls.mapValues(lambda x: list(set(x)))

# COMMAND ----------

#print intitial link and their ranks
print("Iteration:", 0)
print("Initial Links:", rem_dup.collect())
print("Initial Ranks:", current_rank.collect())

#iterate through the links and their ranks 10 times
for i in range(1,11):
    #print current iteration
    print("Iteration:", i)
    #join the link and rank RDDs
    joined_rdd = rem_dup.join(current_rank)
    #print current joined RDD
    print("Joind RDD:", joined_rdd.collect())
    #map new rank values to links via flatMap
    mapped_rank = joined_rdd.flatMap(updateRank)
    #print the new rank for the current iteration
    print("New Contributions:", mapped_rank.collect())
    #combine the rank values by key via reduction
    current_rank = mapped_rank.reduceByKey(lambda x, y: x+y)
    #print the current rankings for each link
    print("Current Rankings:", current_rank.collect())

# COMMAND ----------

#swap key and value (link and rank) so we can sort by highest to lowest rank
final_rank = current_rank.map(lambda x: (x[1], x[0]))
#sore by highest to lowest rank (descending) order
final_rank.sortByKey(False).collect()

# COMMAND ----------

#reads in file
file = sc.textFile('dbfs:///FileStore/tables/lab3Short/') 

# COMMAND ----------

#verify it's the correct file and it was read in correctly
file.collect()

# COMMAND ----------

#create the original link/'link-to' key-value pairs and collect to verify it worked
links = file.map(pairmakerComp)
links.collect()

# COMMAND ----------

#Build the rank RDD

#pull distinct keys and parllelize
distinctfiles = links.keys().distinct().collect()
file_rank = sc.parallelize(distinctfiles)

#map rank value to each key via initial distribution and collect to veriy it was correct
current_rank = file_rank.map(lambda x: (x, 1/len(distinctfiles)))
current_rank.sortByKey().collect()

# COMMAND ----------

#remove duplicate 'link-to' links via mapping values to a set
rem_dup = links.mapValues(lambda x: list(set(x)))

#iterate through the links and their ranks 10 times
for i in range(1,11):
    #join the link and rank RDDs
    joined_rdd = rem_dup.join(current_rank)
    #map new rank values to links via flatMap
    mapped_rank = joined_rdd.flatMap(updateRank)
    #combine the rank values by key via reduction
    current_rank = mapped_rank.reduceByKey(lambda x, y: x+y)
    
current_rank.collect()

# COMMAND ----------

#swap key and value (link and rank) so we can sort by highest to lowest rank
final_rank = current_rank.map(lambda x: (x[1], x[0]))
#sore by highest to lowest rank (descending) order
final_rank.sortByKey(False).collect()

# COMMAND ----------


