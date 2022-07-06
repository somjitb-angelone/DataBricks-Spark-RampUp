# Databricks notebook source
# MAGIC %md # AdventureWorks EDA with Python Dataframes - Part 2
# MAGIC 
# MAGIC ![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png)
# MAGIC 
# MAGIC More examples are available on the Spark website: http://spark.apache.org/examples.html
# MAGIC 
# MAGIC PySpark API documentation: http://spark.apache.org/docs/latest/api/python/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Author: Bryan Cafferky Copyright 07/22/2021

# COMMAND ----------

# MAGIC %md 
# MAGIC ### PySpark Dataframe Methods Documentation
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helpful blog on using PySpark dataframe methods...
# MAGIC https://towardsdatascience.com/the-most-complete-guide-to-pyspark-dataframes-2702c343b2e8#990b

# COMMAND ----------

# MAGIC %md  #### With Databricks, there's no need to import PySpark or create a Spark Context...

# COMMAND ----------

# MAGIC %md ### Warning!!!
# MAGIC 
# MAGIC #### To run this code, you need to have uploaded the files and created the database tables - see Lesson 9 - Creating the SQL Tables on Databricks.  Link in video description to that video.

# COMMAND ----------

# MAGIC %md ## Using Dataframe Methods

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe naming prefix convention:
# MAGIC #### 1st character is s for Spark DF or l for local
# MAGIC #### 2nd character is p for Python (r for R, s for scala)
# MAGIC #### 3rd and 4th character is df for dataframe
# MAGIC #### 5th = _ separator
# MAGIC #### rest is a meaningful name
# MAGIC 
# MAGIC #### spdf_salessummary = a Spark Python dataframe containing sales summary information.

# COMMAND ----------

# DBTITLE 1,Code Cell 1 - The CSV file was uploaded using the Data table create GUI but cancelled without creating a table...
# Use the Spark CSV datasource with options specifying:
# - First line of file is a header
# - Automatically infer the schema of the data

# Note using parenthesis so I can include line breaks for readibility. 

spdf_sales =          (sqlContext.read.format("csv")
                                 .option("header", "true")
                                 .option("inferSchema", "true")
                                 .load("dbfs:/FileStore/tables/FactInternetSales.csv"))

spdf_salesterritory = (sqlContext.read.format("csv")
                            .option("header", "true")
                            .option("inferSchema", "true")
                            .load("dbfs:/FileStore/tables/DimSalesTerritory.csv"))

spdf_sales = spdf_sales.dropna() # drop rows with missing values

# COMMAND ----------

# DBTITLE 1,Code Cell 2 - Line continuation with \ but not recommended by pep 8 
#  See https://stackoverflow.com/questions/7942586/correct-style-for-line-breaks-when-chaining-methods-in-python

spdf_factreason = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true"). \
load("dbfs:/FileStore/tables/FactInternetSalesReason.csv"). \
toDF("SalesOrderNumber", "SalesOrderNumber", "SalesResonKey")

# COMMAND ----------

# DBTITLE 1,Code Cell 3 - Proper Way to Break Up a multiline statement.
spdf_factreason = (sqlContext.read.format("csv")
                              .option("header", "false")
                              .option("inferSchema", "true")
                              .load("dbfs:/FileStore/tables/FactInternetSalesReason.csv") 
                              .toDF("SalesOrderNumber", "SalesOrderLineNumber", "SalesResonKey"))

# COMMAND ----------

# DBTITLE 1,Code Cell 4 - Using show()
spdf_factreason.show(4)

# COMMAND ----------

# DBTITLE 1,Code Cell 5 - Using toPandas() to convert Spark to Pandas
spdf_factreason.toPandas()

# COMMAND ----------

type(spdf_factreason.toPandas())

# COMMAND ----------

# MAGIC %md  
# MAGIC ### Performance Tip:  Use Adaptive Query Execution (AQE)
# MAGIC https://docs.databricks.com/spark/latest/spark-sql/aqe.html

# COMMAND ----------

# DBTITLE 1,Code Cell 6 - You can cache a dataframe to improve performance
spdf_sales.cache() # Cache data for faster reuse 

# COMMAND ----------

# DBTITLE 1,Code Cell 7 - Renaming a column
spdf_sales = spdf_sales.withColumnRenamed("CustomerKey","SalesCustomerKey")

# COMMAND ----------

# DBTITLE 1,Code Cell 8 - Display some rows using take()
display(spdf_sales.take(5))

# COMMAND ----------

# DBTITLE 1,Code Cell  9 - Show Statistics
display(spdf_sales.describe(['SalesAmount', 'UnitPrice']))

# COMMAND ----------

# awproject is the database
spdf_customer = spark.sql('''select CustomerKey, GeographyKey, CommuteDistance, BirthDate, Gender 
                             from awproject.dimcustomer''')

# COMMAND ----------

# DBTITLE 1,Code Cell  10  - Join the PySpark dataframes spdf_customer and spdf_sales 
import pyspark.sql.functions as func

spdf_salescustomer = (spdf_sales.join(
                      spdf_customer, spdf_sales.SalesCustomerKey == spdf_customer.CustomerKey, 'inner')
                      .select('*', func.round('SalesAmount',0))
                      .withColumnRenamed('round(SalesAmount, 0)', 'SalesRounded') 
                      .filter('OrderDateKey > 20101231'))
                      

spdf_salescustomer.limit(3).display()

# COMMAND ----------

# DBTITLE 1,Code Cell  11 - Dataframe aggregations 
display(spdf_salescustomer.groupby('CommuteDistance')
        .agg({'SalesAmount': 'sum'})
        .orderBy('sum(SalesAmount)'))

# COMMAND ----------

# DBTITLE 1,Code Cell 12 - More Aggregations 
display(spdf_salescustomer.groupby('CommuteDistance')
        .agg({'SalesAmount': 'sum', '*': 'count', 'TaxAmt': 'avg'})
        .withColumnRenamed('avg(TaxAmt)', 'Avg_Tax')
        .orderBy('count(1)'))

# COMMAND ----------

# DBTITLE 1,Code Cell 13 - Get Spark DF sample to a pandas dataframe
# drop column SalesCustomerKey and then
# take a sample of the DF so you don't bring too much data back to the cluster driver.
# Doc on sample() method at https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.sample.html

lpdf_salescustomer = (spdf_salescustomer 
                      .drop('SalesCustomerKey')
                      .sample(False, 0.7, 42).toPandas() )

# COMMAND ----------

# DBTITLE 1,Code Cell 14 - Check the data
display(lpdf_salescustomer.head(3))

# COMMAND ----------

type(lpdf_salescustomer)

# COMMAND ----------

# DBTITLE 1,Code Cell 15 - Use local Python library with pandas local dataframe
import pandas as pd

lpdf_salesbygender = lpdf_salescustomer.groupby(['Gender']).agg({'SalesAmount': 'sum'})

lpdf_salesbygender.plot(kind='bar', subplots=True, color='purple')
display()

# COMMAND ----------

# DBTITLE 1,Code Cell 16 - Import plotting libraries
import matplotlib.pyplot as plt
plt.style.use('classic')
%matplotlib inline
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Code Cell 17 - Plot with seaborn - boxplot
import seaborn as sns

with sns.axes_style(style='ticks'):
    g = sns.catplot("CommuteDistance", "SalesAmount", "Gender", data=lpdf_salescustomer, kind="box")
    g.set_axis_labels("SalesTerritoryKey", "Total Sales");

# COMMAND ----------

# DBTITLE 1,Code Cell 18 - Using pandas methods
lpdf_salescustomer.groupby('Gender')['SalesAmount'].sum().round()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Broadcast
# MAGIC 
# MAGIC Such sort of operations is aplenty in Spark where you might want to apply multiple operations to a particular key. But assuming that the data for each key in the Big table is large, it will involve a lot of data movement. And sometimes so much that the application itself breaks. A small optimization then you can do when joining on such big tables(assuming the other table is small) is to broadcast the small table to each machine/node when you perform a join. You can do this easily using the broadcast keyword. This has been a lifesaver many times with Spark when everything else fails.

# COMMAND ----------

# DBTITLE 1,Code Cell 19 - Using broadcast joins
from pyspark.sql.functions import broadcast

spdf_salesterritory = spdf_sales.join(broadcast(spdf_salesterritory), ['SalesTerritoryKey','SalesTerritoryKey'], how='left')

# COMMAND ----------

# DBTITLE 1,Code Cell 20 - Show results
spdf_salesterritory[['SalesOrderNumber', 'SalesAmount',  'SalesTerritoryGroup', 'SalesTerritoryRegion']].display()
