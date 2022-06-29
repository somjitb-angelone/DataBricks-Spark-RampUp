-- Databricks notebook source
-- MAGIC %md ## Author: Bryan Cafferky  Copyright 3/19/2021
-- MAGIC 
-- MAGIC ### For demonstration purposes only.
-- MAGIC ### Not intended for production use.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Exploring Adventureworks Sales Data with SQL  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### The awproject database was created in a prior video which will be in this video descriptionn.

-- COMMAND ----------

-- DBTITLE 1,Change to the aw database context
USE somjit_practice

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Let's look at the product data...
-- MAGIC 
-- MAGIC - ProductCategoryKey from dimproductcategory
-- MAGIC - ProductSubcategoryKey from dimproductsubcategory
-- MAGIC - ProductKey from dimproduct
-- MAGIC - EnglishProductCategoryName renamed as Category from dimproductcategory
-- MAGIC - EnglishProductSubcategoryName renamed as Subcategory from dimproductsubcategory
-- MAGIC - ModelName as Model from dimproduct

-- COMMAND ----------

SELECT p.ProductKey, 
s.ProductSubcategoryKey, 
c.ProductCategoryKey, 
EnglishProductCategoryName as Category, 
EnglishProductSubcategoryName as Subcategory, 
ModelName as Model
FROM       dimproduct                    p
INNER JOIN dimproductsubcategory         s
ON (p.ProductSubcategoryKey = s.ProductSubcategoryKey)
INNER JOIN dimproductcategory            c
ON (s.ProductCategoryKey = c.ProductCategoryKey)
WHERE p.Status = 'Current' OR p.Status = 'NULL' 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create a SQL table called *t_productinfo* that contains the data from the prior query

-- COMMAND ----------

-- DBTITLE 1,Creating a table to simplify queries - Left Join
DROP TABLE IF EXISTS t_productinfo;

CREATE TABLE t_productinfo as
SELECT p.ProductKey, s.ProductSubcategoryKey, 
c.ProductCategoryKey, 
EnglishProductCategoryName as Category, 
EnglishProductSubcategoryName as Subcategory, 
ModelName as Model
FROM       dimproduct                    p
INNER JOIN dimproductsubcategory         s
ON (p.ProductSubcategoryKey = s.ProductSubcategoryKey)
INNER JOIN dimproductcategory           c
ON (s.ProductCategoryKey = c.ProductCategoryKey)
WHERE p.Status = 'Current' OR p.Status = 'NULL';

-- COMMAND ----------

-- DBTITLE 1,Using the product table
SELECT DISTINCT Model, SubCategory, Category, count(*) as NumProducts
FROM t_productinfo 
GROUP BY Model, SubCategory, Category
ORDER BY NumProducts DESC

-- COMMAND ----------

DESCRIBE TABLE t_productinfo

-- COMMAND ----------

SHOW CREATE TABLE dimcustomer

-- COMMAND ----------

-- DBTITLE 1,Getting meta data about a table
SHOW CREATE TABLE t_productinfo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # End of SQL EDA Video 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Start of SQL EDA Video 2

-- COMMAND ----------

use somjit_practice

-- COMMAND ----------

-- DBTITLE 1,Create Customer view with a calculated column
CREATE OR REPLACE VIEW v_customer as
SELECT CustomerKey, GeographyKey, Gender,  YearlyIncome, BirthDate, TotalChildren,
NumberChildrenAtHome, 
EnglishEducation as Education, HouseOwnerFlag, 
NumberCarsOwned, DateFirstPurchase, 
EnglishOccupation as Occupation, 
CommuteDistance, 
int((DATEDIFF(CURRENT_DATE, birthdate))/365) as Age 
FROM dimcustomer;

-- COMMAND ----------

SELECT * from v_customer limit 2

-- COMMAND ----------

-- MAGIC %md ## Create a SQL view named vsalesinfo that contains the followig data.
-- MAGIC - SalesAmount from factinternetsales
-- MAGIC - All the columns from t_productinfo (the view we created earlier)
-- MAGIC - from dimcustomer, get Education
-- MAGIC - from dimcustomer, get Gender
-- MAGIC - from dimcustomer, get YearlyIncome as Salary
-- MAGIC - from dimcustomer, if the customer has any children (NumberChildrenAtHome > 0), set to 'Y', else set to 'N' and call this column 'HasChildren'
-- MAGIC - from dimcustomer, get HomeOwnerFlag as HomeOwner
-- MAGIC - from dimcustomer, AgeBand as 
-- MAGIC ``` 
-- MAGIC CASE  WHEN age < 18 then 'Minor'
-- MAGIC                        WHEN age between 19 and 29 then 'Young'
-- MAGIC                        WHEN age between 30 and 39 then 'Middle'
-- MAGIC                        WHEN age between 40 and 49 then 'Late Middle'
-- MAGIC                        WHEN age > 50 then 'Golden'
-- MAGIC                        ELSE 'Other' END as AgeBand"
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,Creating a view of sales information - inner joins
CREATE OR REPLACE VIEW v_salesinfo as 
SELECT  OrderDateKey, DueDateKey, s.CustomerKey, PromotionKey, SalesTerritoryKey,
SalesAmount, vp.*, Gender, YearlyIncome as Salary, OrderQuantity, DiscountAmount, TotalProductCost, TaxAmt, 
CASE WHEN NumberChildrenAtHome > 0 THEN 'Y' ELSE 'N' END as HasChildren, 
HouseOwnerFlag as HomeOwner, 
CASE WHEN Age < 18 THEN 'Minor' 
     WHEN Age BETWEEN 19 AND 29 THEN 'Young' 
     WHEN Age BETWEEN 30 AND 39 THEN 'Middle' 
     WHEN Age BETWEEN 40 AND 49 THEN 'Late Middle' 
     WHEN Age > 50 THEN 'Golden' 
     ELSE 'Other' END as AgeBand, 
vc.Education, vc.NumberCarsOwned, vc.CommuteDistance,
d.FiscalYear, d.FiscalQuarter, 
d.EnglishMonthName as Month, MonthNumberOfYear, d.CalendarYear
FROM factinternetsales                s
INNER JOIN v_customer                 vc
ON (s.CustomerKey = vc.CustomerKey)
INNER JOIN t_productinfo              vp
ON (s.ProductKey = vp.ProductKey)
INNER JOIN dimdate                    d
ON (s.OrderDateKey = d.DateKey)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Using Common Table Expressions

-- COMMAND ----------

-- DBTITLE 1,Using a CTE (Common Table Expression) to determine what years do we have complete data for?
WITH cte_check_months as
(
SELECT FiscalYear, MonthNumberOfYear, COUNT(*) as NumSales
FROM v_salesinfo
GROUP BY  FiscalYear, MonthNumberOfYear
ORDER BY  FiscalYear, MonthNumberOfYear
)

SELECT FiscalYear, COUNT(*) as Count, SUM(NumSales)
FROM cte_check_months
GROUP BY FiscalYear
ORDER BY FiscalYear

-- COMMAND ----------

-- DBTITLE 1,Get sales by customer education level from the view vsalesinfo
SELECT Education, Category, ROUND(SUM(SalesAmount),0) as TotalSales
FROM v_salesinfo
GROUP BY Category, Education 
ORDER BY TotalSales DESC

-- COMMAND ----------

-- DBTITLE 1,Using the CASE statement to translate values
with cte_countrycodes as
(
SELECT salesterritorykey,
       CASE SalesTerritoryCountry 
       WHEN 'United States' THEN 'USA' 
       WHEN 'United Kingdom' THEN 'GBR' 
       WHEN 'Canada' THEN 'CAN' 
       WHEN 'France' THEN 'FRA' 
       WHEN 'Australia' THEN 'AUS' 
       WHEN 'Germany' THEN 'DEU' 
       ELSE 'NA' END as CountryCD
from dimsalesterritory t
)

SELECT CountryCD, SalesAmount
FROM v_salesinfo       s
JOIN cte_countrycodes t
ON (s.salesterritorykey = t.salesterritorykey)
WHERE CountryCD <> "NA"

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### Views are great but we want to keep this around permanently.  
-- MAGIC ### Let's saves the views to tables.

-- COMMAND ----------

-- DBTITLE 1,Save vcustomer
DROP TABLE IF EXISTS  t_customerinfo;
   
CREATE TABLE t_customerinfo 
as
SELECT * FROM v_customer;

-- COMMAND ----------

-- DBTITLE 1,Save vsalesinfo
DROP TABLE IF EXISTS  t_salesinfo;
   
CREATE TABLE t_salesinfo 
as
SELECT * FROM v_salesinfo;

-- COMMAND ----------

SELECT * FROM t_salesinfo limit 3

-- COMMAND ----------

-- DBTITLE 1,Drop the views
DROP VIEW IF EXISTS v_customer;
DROP VIEW IF EXISTS v_salesinfo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exercises for Spark Fitness...
-- MAGIC 
-- MAGIC Do the practice exercises below to test your Spark SQL knowledge!

-- COMMAND ----------

DESCRIBE TABLE t_customerinfo

-- COMMAND ----------

DESCRIBE t_salesinfo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### 1) Write a SQL query that returns total sales amount (SalesAmount) by the customer's number of children.
-- MAGIC - total of SalesAmount from t_salesinfo named as TotalSales
-- MAGIC - grouped by NumberChildrenAtHome from t_customerinfo

-- COMMAND ----------

-- DBTITLE 1,Enter your answer in the cell below

SELECT c.NumberChildrenAtHome as numChildren, SUM(s.SalesAmount) as TotalSales
FROM t_salesinfo s
INNER JOIN
t_customerinfo c
ON (s.CustomerKey = c.CustomerKey)
GROUP BY numChildren;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Click on Show code below to see the solution...

-- COMMAND ----------

SELECT c.NumberChildrenAtHome, SUM(SalesAmount) as TotalSales
FROM t_customerinfo    c
INNER JOIN t_salesinfo s
ON (c.CustomerKey = s.CustomerKey)
GROUP BY c.NumberChildrenAtHome

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### 2) Modify the query above to...
-- MAGIC - round TotalSales amount to a whole number, i.e. no decimal
-- MAGIC - Sort TotalSales from High to Low (note:  You need to sort by SUM(TotalSales))

-- COMMAND ----------

-- DBTITLE 1,Enter your answer in the cell below
SELECT c.NumberChildrenAtHome as numChildren, ROUND(SUM(s.SalesAmount)) as TotalSales
FROM t_salesinfo s
INNER JOIN
t_customerinfo c
ON (s.CustomerKey = c.CustomerKey)
GROUP BY numChildren
ORDER BY TotalSales DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Click on Show code below to see the solution...

-- COMMAND ----------

-- DBTITLE 1,Solution


SELECT c.NumberChildrenAtHome, ROUND(SUM(SalesAmount), 0) as TotalSales
FROM t_customerinfo    c
INNER JOIN t_salesinfo s
ON (c.CustomerKey = s.CustomerKey)
GROUP BY c.NumberChildrenAtHome
ORDER BY SUM(SalesAmount) DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### 3) What is the average sales by fiscal year by gender?
-- MAGIC - SalesAmount from t_salesinfo - Note:  AVG() is the SQL function for average.  Name colums TotalSales
-- MAGIC - Gender from t_customerinfo 
-- MAGIC - round the average SalesAmount to whole numbers
-- MAGIC - Group and Sort by FiscalYear, Gender, and Average SalesAmount

-- COMMAND ----------

-- DBTITLE 1,Enter your answer in the cell below
SELECT s.FiscalYear, c.Gender, ROUND(AVG(s.SalesAmount)) as avgSales
FROM t_salesinfo            s
INNER JOIN t_customerinfo   c
ON (s.CustomerKey = c.CustomerKey)
GROUP BY s.FiscalYear, c.Gender
ORDER BY s.FiscalYear, c.Gender, avgSales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Click on Show code below to see the solution...

-- COMMAND ----------

SELECT s.FiscalYear, c.Gender, ROUND(AVG(SalesAmount)) as TotalSales
FROM t_salesinfo            s
INNER JOIN t_customerinfo   c
ON (s.CustomerKey = c.CustomerKey)
GROUP BY s.FiscalYear, c.Gender
ORDER BY s.FiscalYear, c.Gender, AVG(SalesAmount)
