-- Databricks notebook source
-- MAGIC %md ## Author: Bryan Cafferky  Copyright 5/2021
-- MAGIC 
-- MAGIC ### For demonstration purposes only.
-- MAGIC ### Not intended for production use.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Using Spark SQL Window functions...

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### The awproject database was created in a prior video which will be in this video descriptionn.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Apache Spark Window Functions documentation
-- MAGIC #### https://spark.apache.org/docs/2.3.1/api/sql/index.html
-- MAGIC #### http://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html
-- MAGIC #### https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-functions-windows.html
-- MAGIC 
-- MAGIC ### Examples:
-- MAGIC #### http://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Key Terms
-- MAGIC 
-- MAGIC #### OVER — Identifies the start of a window function block of code. 
-- MAGIC #### PARTITION – The data grouping.  The window function works within each partition. In the old days, we called
-- MAGIC ####             this a control-break.
-- MAGIC #### ORDER BY — Sorts the rows by the specified colmumn which is critical for things like running totals.

-- COMMAND ----------

-- DBTITLE 1,Change to the aw database context
USE awproject

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create some views to use in the queries...

-- COMMAND ----------

CREATE OR REPLACE VIEW v_salessummary as 
SELECT YEAR(OrderDate) as SalesYear,
       MONTH(OrderDate) as SalesMonth, 
       ROUND(SUM(SalesAmount)) as TotalSales 
FROM factinternetsales
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT * FROM v_salessummary limit 2

-- COMMAND ----------

CREATE OR REPLACE VIEW v_productcatalog as 
SELECT EnglishProductCategoryName as Category, EnglishProductName as Product, ROUND(SUM(SalesAmount)) as SalesAmount
FROM       factinternetsales          s
INNER JOIN dimproduct                 p
ON (s.ProductKey = p.ProductKey)
INNER JOIN dimproductsubcategory      psc
ON (p.ProductSubCategoryKey = psc.ProductSubCategoryKey)
INNER JOIN dimproductcategory         pc
ON (psc.ProductCategoryKey = pc.ProductCategoryKey)
GROUP BY EnglishProductCategoryName, EnglishProductName;

SELECT * FROM v_productcatalog limit 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Window Function Examples

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Goal:  Get results with a cumulative total for each group.

-- COMMAND ----------

SELECT SalesYear, SalesMonth, TotalSales, Sum(TotalSales) 
  OVER ( PARTITION BY (SalesYear) ORDER BY SalesMonth ROWS BETWEEN unbounded preceding AND CURRENT ROW ) cumsum
FROM v_salessummary
WHERE SalesYear > 2010

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Goal:  Get results with a cumulative average for 1 row before and 1 row after the current row.

-- COMMAND ----------

SELECT SalesYear, SalesMonth, AVG(SalesMonth)  
  OVER ( ORDER BY SalesMonth ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING ) rollingavg
FROM v_salessummary
WHERE SalesYear = 2011

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Goal: Get the top 2 selling products by product category.

-- COMMAND ----------

SELECT
  Category,
  Product,
  SalesAmount,
  rank
FROM (
  SELECT
    Category,
    Product,
    SalesAmount,
    dense_rank() OVER (PARTITION BY Category ORDER BY SalesAmount DESC) as rank
  FROM v_productcatalog) 
WHERE
  rank <= 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Same query simplified with CTE's

-- COMMAND ----------

with cte_window_data 
(
 SELECT Category, Product, SalesAmount,
    dense_rank() OVER (PARTITION BY Category ORDER BY SalesAmount DESC) as rank
 FROM v_productcatalog  
)

SELECT
  Category,
  Product,
  SalesAmount,
  rank
FROM cte_window_data 
WHERE
  rank <= 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Goal: Get the lowest and highest SalesAmount for each customer displayed next to each sale for the customer.

-- COMMAND ----------

SELECT CustomerKey, YEAR(OrderDate) as OrderYear, SalesAmount, 
       MIN(SalesAmount) OVER (PARTITION BY CustomerKey) AS min, 
       MAX(SalesAmount) OVER (PARTITION BY CustomerKey) AS max  
FROM factinternetsales  
ORDER BY CustomerKey, SalesAmount

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ntile - breaks up a set into subsets.
-- MAGIC 
-- MAGIC https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions101.htm#:~:text=NTILE%20is%20an%20analytic%20function,are%20numbered%201%20through%20expr%20.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Explanation
-- MAGIC https://docs.cloudera.com/cdp-private-cloud-base/7.1.6/impala-sql-reference/topics/impala-analytic-functions-ntile.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Goal: Break up the product list into 40 equally distributed subsets by value, like quartiles but for any number.

-- COMMAND ----------

SELECT ProductKey, EnglishProductName as Product, CAST(StandardCost as DECIMAL(10,2)) as StandardCost, ntile(40) 
      OVER(ORDER BY CAST(StandardCost as DECIMAL(10,2))) as ntile 
FROM dimproduct  
WHERE StandardCost > 0
ORDER BY ntile, 3
