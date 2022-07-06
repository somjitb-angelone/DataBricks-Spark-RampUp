# Databricks notebook source
# MAGIC %md # PySpark - Using Resilient Distributed Datasets (RDDs)
# MAGIC 
# MAGIC ![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png)
# MAGIC 
# MAGIC More examples are available on the Spark website: http://spark.apache.org/examples.html
# MAGIC 
# MAGIC PySpark API documentation: http://spark.apache.org/docs/latest/api/python/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Author: Bryan Cafferky Copyright 
# MAGIC 
# MAGIC ### For demonstration only.  Not intended for production use.

# COMMAND ----------

# MAGIC %md  #### With Databricks, there's no need to import PySpark or create a Spark Context...

# COMMAND ----------

# MAGIC %md ### Using RDDs with Python
# MAGIC 
# MAGIC Code adpated from https://spark.apache.org/docs/latest/rdd-programming-guide.html#basics
# MAGIC #### and from
# MAGIC https://www.analyticsvidhya.com/blog/2016/10/using-pyspark-to-perform-transformations-and-actions-on-rdd/

# COMMAND ----------

# DBTITLE 1,Spark Context
sc

# COMMAND ----------

# DBTITLE 1,Spark 
spark

# COMMAND ----------

# DBTITLE 1,Load file into RDD
poem_rdd1 = sc.textFile("dbfs:/FileStore/hamlet.txt")

# COMMAND ----------

poem_rdd1

# COMMAND ----------

poem_rdd1.take(2)

# COMMAND ----------

poem_rdd1.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations:  map()

# COMMAND ----------

# DBTITLE 1,Map and Reduce
lineLengths = poem_rdd1.map(lambda s: len(s))         # transformation
totalLength = lineLengths.reduce(lambda a, b: a + b)  # action
print(totalLength)

# COMMAND ----------

print(lineLengths)

# COMMAND ----------

# DBTITLE 1,Get the rdd item count
my_rdd  = sc.parallelize([1, 2, 3, 4, 5]) # creates RDD
my_rdd.count()  # action

# COMMAND ----------

print(my_rdd)  # makes no sense

# COMMAND ----------

print(my_rdd.take(3))  # action returns elements to the driver

# COMMAND ----------

# DBTITLE 1,Show the number of partitions
poem_rdd1.getNumPartitions() 

# COMMAND ----------

# DBTITLE 1,Using a defined function with map
def transfunc(lines):
      lines = lines.lower()
      lines = lines.split()
      return lines
    
poem_rdd2 = poem_rdd1.map(transfunc)
poem_rdd2.take(5)

# COMMAND ----------

# DBTITLE 1,Using flatmap to flatten out the rdd
poem_rdd3 = poem_rdd1.flatMap(transfunc)
poem_rdd3.take(15)

# COMMAND ----------

# DBTITLE 1,Get distinct values in an rdd
poem_rdd3.distinct().take(5)

# COMMAND ----------

# DBTITLE 1,Using filter to apply a search condition
skipwords = ['to','the','of']
poem_rdd4 = poem_rdd3.filter(lambda x: x not in skipwords)
poem_rdd4.take(10)

# COMMAND ----------

# DBTITLE 1,Getting statistics on an rdd
numbers_rdd = sc.parallelize(range(1,500))
numbers_rdd.max(), numbers_rdd.min(), numbers_rdd.sum(),numbers_rdd.variance(),numbers_rdd.stdev()
