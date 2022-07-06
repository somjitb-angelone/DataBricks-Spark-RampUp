# Databricks notebook source
# MAGIC %md # AdventureWorks EDA with Python and SQL
# MAGIC 
# MAGIC ![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png)
# MAGIC 
# MAGIC More examples are available on the Spark website: http://spark.apache.org/examples.html
# MAGIC 
# MAGIC PySpark API documentation: http://spark.apache.org/docs/latest/api/python/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Author: Bryan Cafferky Copyright 07/07/2021

# COMMAND ----------

# MAGIC %md ### Why use SQL with PySpark?
# MAGIC 
# MAGIC #### 1) Its easy to load PySpark dataframe using SQL.
# MAGIC #### 2) SQL is very expressive and powerful.
# MAGIC #### 3) Everyone knows SQL.
# MAGIC #### 4) SQL Tables are the ideal place to store and query data.
# MAGIC #### 5) SQL Tables and Views can be accessed from any Spark language.

# COMMAND ----------

# MAGIC %md  #### With Databricks, there's no need to import PySpark or create a Spark Context...

# COMMAND ----------

# MAGIC %md ### Warning!!!
# MAGIC 
# MAGIC #### To run this code, you need to have uploaded the files and created the database tables - see Lesson 9 - Creating the SQL Tables on Databricks.  Link in video description to that video.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2 Types of dataframes
# MAGIC ###- Local (pandas)
# MAGIC ###- Spark (distributed)

# COMMAND ----------

# MAGIC %md ## Using Spark SQL from Python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe naming prefix convention:
# MAGIC #### 1st character is s for Spark DF, l is for local
# MAGIC #### 2nd character is p for Python (r for R, s for Scala)
# MAGIC #### 3rd and 4th character is df for dataframe
# MAGIC #### 5th = _ separator
# MAGIC #### rest is a meaningful name
# MAGIC 
# MAGIC #### spdf_salessummary = a Spark Python dataframe containing sales summary information.

# COMMAND ----------

# DBTITLE 1,Code Cell 1 - Use Spark SQL to load a PySpark dataframe from the factinternetsales...
spark.sql('use somjit_practice')
spdf_salesinfo = spark.sql('select * from factinternetsales').dropna()

# COMMAND ----------

spdf_salesinfo

# COMMAND ----------

# DBTITLE 1,Code Cell 2 - Use PySpark dataframe with Notebook Display
display(spdf_salesinfo)

# COMMAND ----------

# DBTITLE 1,Code Cell 3 - SQL Query to extract Customer in the US only...
getcustgeoqry = '''
select CustomerKey, CountryRegionCode, EnglishCountryRegionName as CountryName, 
StateProvinceCode, StateProvinceName, City, PostalCode, UPPER(City) as UpperCity
from  dimcustomer         c
inner join  dimgeography  g
on (c.geographykey = g.geographykey) 
where countryregioncode = 'US'
'''

# COMMAND ----------

# DBTITLE 1,Code Cell 4  - Run the customer query storing the results in a dataframe
spdf_custgeo_us = spark.sql(getcustgeoqry)
display(spdf_custgeo_us)

# COMMAND ----------

# DBTITLE 1,Code Cell 5 - Show columns
spdf_salesinfo.columns

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Code Cell 6 - Adding a column to a dataframe
spdf_salesinfo = spdf_salesinfo.withColumn('TaxRate',  QR5509.TaxAmt / spdf_salesinfo.SalesAmount)

# COMMAND ----------

# DBTITLE 1,Code Cell 7 - Showing the new column
display(spdf_salesinfo[['SalesAmount', 'TaxAmt', 'TaxRate']])

# COMMAND ----------

# MAGIC %md #### Documention on SQL CREATE TABLE statement 
# MAGIC https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-datasource.html
# MAGIC 
# MAGIC #### Note:  Delta tables do not support all CREATE TABLE options. 

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS t_salesinfoextract

# COMMAND ----------

# DBTITLE 1,Code Cell 8 - Create a table
spark.sql('''
create or replace table t_salesinfoextract 
using delta 
location '/aw/delta/tables/t_salesinfoextract'
as
select CustomerKey, CountryRegionCode, EnglishCountryRegionName as CountryName, 
StateProvinceCode, StateProvinceName, City, PostalCode, UPPER(City) as UCASECity 
from  dimcustomer         c
inner join  dimgeography  g
on (c.geographykey = g.geographykey) 
where countryregioncode = 'US'
''')

# COMMAND ----------

spark.sql('''select * from t_salesinfoextract limit 3''')

# COMMAND ----------

# DBTITLE 1,Code Cell 9 - Query the new table
spark.sql('''select * from t_salesinfoextract limit 3''').collect()

# COMMAND ----------

# DBTITLE 1,Code Cell 10 - Display Query Output - list is returned
lpdf_salesinfo = spark.sql('''select * from t_salesinfoextract limit 3''').collect()
display(lpdf_salesinfo)

# COMMAND ----------

# DBTITLE 1,Code Cell 11 - What type of object is this?
type(lpdf_salesinfo)

# COMMAND ----------

# DBTITLE 1,Code Cell 12 - Convert the Spark dataframe to pandas
lpdf_salesinfo = spark.sql('''select * from t_salesinfoextract limit 3''').toPandas()
lpdf_salesinfo

# COMMAND ----------

lpdf_salesinfo.columns

# COMMAND ----------

# DBTITLE 1,Code Cell 13 - Confirm this is a pandas dataframe
type(lpdf_salesinfo)

# COMMAND ----------

# MAGIC %md #### Documentation on Dataframe saveAsTable() method.
# MAGIC http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.saveAsTable.html

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS salesdatafrompython1

# COMMAND ----------

# DBTITLE 1,Code Cell 14 - Spark Dataframe writer saveAsTable
import pyspark
from pyspark.sql import DataFrameWriter

spdf_salesinfo_writer = pyspark.sql.DataFrameWriter(spdf_salesinfo)
spdf_salesinfo_writer.partitionBy('SalesTerritoryKey').saveAsTable('salesinfopython1', 
                                                                   format='delta', 
                                                                   mode='overwrite', 
                                                                   path='/aw/delta/tables/salesinfopython1')

# COMMAND ----------

# MAGIC %fs ls /aw/delta/tables/salesinfopython1

# COMMAND ----------

# DBTITLE 1,Code Cell 15
# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED salesinfopython1

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS salesdatafrompython2

# COMMAND ----------

# DBTITLE 1,Code Cell 16 - Save table without explicitly using dataframewriter
spdf_salesinfo.write.mode("overwrite").format("parquet").option("path","/aw/parquet/tables/salesinfopython2").saveAsTable("salesinfopython2")

# COMMAND ----------

# MAGIC %fs ls /aw/parquet/tables/salesinfopython2

# COMMAND ----------

# DBTITLE 1,Code Cell 17
# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED salesinfopython2

# COMMAND ----------

# DBTITLE 1,Code Cell 18 - Spark Dataframe createOrReplaceTempView
spdf_salesinfo.createOrReplaceTempView("tv_salesinfo")

# COMMAND ----------

display(spark.sql('''SELECT * FROM tv_salesinfo LIMIT 20'''))


# COMMAND ----------

# DBTITLE 1,Code Cell 19 - Using the view from SQL 
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM tv_salesinfo limit 3

# COMMAND ----------

# DBTITLE 1,Code Cell 20 -Using the table from R
# MAGIC %r
# MAGIC library(SparkR)
# MAGIC 
# MAGIC display(sql('select * from tv_salesinfo limit 3'))

# COMMAND ----------

# DBTITLE 1,Code Cell 21 - Using the table from Scala
# MAGIC %scala 
# MAGIC display(spark.sql("select * from tv_salesinfo limit 3"))

# COMMAND ----------

# DBTITLE 1,Code Cell  22 - SQL Aggregation Queries
spdfSalesSummary = spark.sql('''
select SalesTerritoryKey, count(*), sum(SalesAmount) as TotalSales, avg(SalesAmount) as AverageSales 
from  t_salesinfo      
group by SalesTerritoryKey
''')

display(spdfSalesSummary)
