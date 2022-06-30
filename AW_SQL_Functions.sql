-- Databricks notebook source
-- MAGIC %md ## Author: Bryan Cafferky  Copyright 4/25/2021
-- MAGIC 
-- MAGIC ### For demonstration purposes only.
-- MAGIC ### Not intended for production use.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Using Spark SQL functions...

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Apache Spark Functions documentation
-- MAGIC ### https://spark.apache.org/docs/2.3.1/api/sql/index.html
-- MAGIC ### https://spark.apache.org/docs/3.1.1/sql-ref-functions.html

-- COMMAND ----------

-- DBTITLE 1,Change to the aw database context
USE somjit_practice

-- COMMAND ----------

DESCRIBE TABLE factinternetsales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Scalar functions - operate on one row at a time.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Function: datediff
-- MAGIC 
-- MAGIC #### datediff(endDate, startDate) - Returns the number of days from startDate to endDate.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Function: format_string
-- MAGIC 
-- MAGIC #### format_string(strfmt, obj, ...) - Returns a formatted string from printf-style format strings.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Function: date_format
-- MAGIC #### date_format(timestamp, fmt) - Converts timestamp to a value of string in the format specified by the date format fmt.
-- MAGIC https://spark.apache.org/docs/3.1.1/sql-ref-datetime-pattern.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Function: to_json
-- MAGIC 
-- MAGIC #### to_json(expr[, options]) - Returns a json string with a given struct value

-- COMMAND ----------

SELECT datediff(ShipDate, OrderDate) as DaysToShip,
       format_string("Ordered on %s and shipped on %s", 
                     date_format(OrderDate, "y-M-d"), date_format(ShipDate, "yyyy-MM-dd")) as Message,
       to_json(named_struct('DateShipped', ShipDate)) as json
FROM factinternetsales limit 5

-- COMMAND ----------

SELECT OrderDate,
       year(OrderDate) as Year,
       dayofmonth(OrderDate) as MonthDay,
       dayofyear(OrderDate) as DayOfYear,
       CASE  WHEN  dayofyear(OrderDate) <= 100 THEN 'Early in the year'
             WHEN  dayofyear(OrderDate) BETWEEN 101 AND 200 THEN 'Middle of year' 
             ELSE 'Late in the year'
       END as TimeOfYear
FROM factinternetsales 
ORDER BY SalesOrderNumber
LIMIT 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Aggregate Functions - Operates over a group of values 
-- MAGIC https://spark.apache.org/docs/3.1.1/sql-ref-functions-builtin.html#aggregate-functions

-- COMMAND ----------

-- If there are spaces in the column header, enclose the label in backticks 
SELECT year(OrderDate), 
       sum(SalesAmount) as `Total Sales`,
       round(sum(SalesAmount), 0) as `Total Sales (rounded)`,
       count(*) as `Order Count`, 
       round(avg(SalesAmount), 0) as `Avg Sales`,
       max(SalesAmount) as `Highest Sale`,
       min(SalesAmount) as `Lowest Sale`,
       round(stddev_pop(SalesAmount), 2) as `Std Dev`,
       round(var_pop(SalesAmount), 2) as `Variance`,
       round(corr(SalesAmount, UnitPrice), 4) as corr
FROM factinternetsales 
GROUP BY 1
HAVING sum(SalesAmount) > 100000
ORDER BY 1

-- COMMAND ----------

DESCRIBE TABLE factinternetsales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Test Your Skills

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1)
-- MAGIC #### Query factinternetsales showing the OrderDate, the first five characters of the SalesOrderNumber,  
-- MAGIC #### the length of SalesOrderNumber, and the SalesAmount.

-- COMMAND ----------

--  Code your answer here...
SELECT OrderDate,
       LEFT(SalesOrderNumber, 5) as `First 5 Characters`,
       LENGTH(SalesOrderNumber) as `Character Length`,
       SalesAmount as `Sales Amount`
       
FROM factinternetsales
LIMIT 20;

-- COMMAND ----------

-- Show this cell for the solution.
-- ============================================
SELECT OrderDate, substring(SalesOrderNumber, 1, 5), length(SalesOrderNumber), SalesAmount 
FROM factinternetsales 
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2)
-- MAGIC #### Get the total SalesAmount, average SalesAmount, and the 
-- MAGIC #### total SalesAmount minus the average SalesAmount with the column heading Sales Over/Under Avg.
-- MAGIC #### grouped by SalesTerritoryKey returning only results having the total sales amount greater than 6000.

-- COMMAND ----------

-- Code your answer here.

SELECT SalesTerritoryKey as `Territory Key`,
       SUM(SalesAmount) as `Total Sales`,
       AVG(SalesAmount) as `Average Sales`,
       (SUM(SalesAmount) - AVG(SalesAmount)) as `Sales Over Avg`
       
FROM factinternetsales
GROUP BY SalesTerritoryKey
HAVING `Total Sales` > 6000
LIMIT 10;

-- COMMAND ----------

-- Show this cell for the solution.
-- ============================================

SELECT sum(SalesAmount), avg(SalesAmount), (sum(SalesAmount) - avg(SalesAmount)) as `Sales Over/Under Avg` 
FROM factinternetsales 
GROUP BY SalesTerritoryKey
HAVING sum(SalesAmount) > 6000

-- COMMAND ----------


