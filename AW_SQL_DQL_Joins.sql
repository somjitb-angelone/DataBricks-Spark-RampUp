-- Databricks notebook source
-- MAGIC %md ### Author: Bryan Cafferky 
-- MAGIC 
-- MAGIC ### Using SQL Joins
-- MAGIC 
-- MAGIC #### Good to use different join types depending on the nature of the data.
-- MAGIC ### Left and Outer joins are useful to check data integrity.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## The tables were created in lesson 10 of the series...

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC USE somjit_practice

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL Joins By Type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Inner Join
-- MAGIC 
-- MAGIC Inner Join only returns rows that have a match on the left and right side of the join.
-- MAGIC In many cases, tables are assumed to always have matching rows on both sides.
-- MAGIC But you need to test this hypothesis.

-- COMMAND ----------

DESCRIBE dimproduct;

-- COMMAND ----------

DESCRIBE dimproductsubcategory;

-- COMMAND ----------

-- DBTITLE 1,Inner Join
-- Join the left (first) table to the right (second) returning only rows that match. You should not get any null join keys. 
SELECT     p.ProductKey, p.EnglishProductName as Product, psc.EnglishProductSubcategoryName as Subcategory 
FROM       dimproduct                 p
INNER JOIN dimproductsubcategory      psc
ON (p.ProductSubcategoryKey = psc.ProductSubcategoryKey)
LIMIT 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Left Join
-- MAGIC 
-- MAGIC Left Join returns all rows on the left side of the join regardless of whether there is a match on the  right side.
-- MAGIC Rows on the right side with no match on the left are NOT returned.

-- COMMAND ----------

-- DBTITLE 1,Left Join
-- Join the left (first) table to the right (second) table and return all rows on the 
-- left filling in the right columns with nulls if there is no match.
SELECT     p.ProductKey, p.EnglishProductName as Product, p.ProductSubcategoryKey,
           psc.EnglishProductSubcategoryName as Subcategory 
FROM       dimproduct                 p
LEFT JOIN dimproductsubcategory      psc
ON (p.ProductSubcategoryKey = psc.ProductSubcategoryKey)
WHERE psc.ProductSubcategoryKey IS NULL
LIMIT 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Right Join
-- MAGIC 
-- MAGIC Right Join returns all rows on the right side of the join regardless of whether there is a match on the left side.
-- MAGIC Rows on the left side with no match on the left are NOT returned.
-- MAGIC 
-- MAGIC Note:  Since you can accomplish the same thing with a left join by just putting the right side table on the left,
-- MAGIC I never use this join.

-- COMMAND ----------

-- DBTITLE 1,Right Join
-- Join the table on the right (second table) with the table on the left (first table)
-- Return all rows from the right whether or not there is a match on the left. 
SELECT      p.ProductKey, p.EnglishProductName as Product, psc.EnglishProductSubcategoryName as Subcategory 
FROM        dimproductsubcategory                 psc
RIGHT JOIN  dimproduct                            p
ON (psc.ProductSubcategoryKey = p.ProductSubcategoryKey)
WHERE psc.ProductSubcategoryKey IS NULL
LIMIT 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Full Outer Join
-- MAGIC 
-- MAGIC Returns all rows from both sides of the join.  
-- MAGIC Values on the missing side are returned as NULLs.
-- MAGIC 
-- MAGIC Rarely used and may incur high overhead.  But may be a good way to validate data quality.

-- COMMAND ----------

-- DBTITLE 1,Check the product categories
select EnglishProductSubcategoryName from  dimproductsubcategory limit 5

-- COMMAND ----------

-- DBTITLE 1,Full Outer Join
-- Join both tables on the key and return all rows from both tables filling in NULLs for the columns on the table with no match.
SELECT     p.ProductKey, p.EnglishProductName as Product, p.ProductSubcategoryKey, psc.EnglishProductSubcategoryName as Subcategory 
FROM       dimproduct                 p
FULL OUTER JOIN dimproductsubcategory      psc
ON (p.ProductSubcategoryKey = psc.ProductSubcategoryKey)
-- WHERE p.ProductSubcategoryKey <> 'NULL'
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Left Semi Join
-- MAGIC 
-- MAGIC Returns values from the left side of the relation that has a match with the right. It is also referred to as a left semi join.
-- MAGIC Like a inner join but only returns the left side columns.

-- COMMAND ----------

-- DBTITLE 1,Left Semi Join
-- Join the left (first) table to the right (second) table and return only the left table 
-- columns and only if there is a match. 
SELECT         p.ProductKey, p.EnglishProductName as Product
-- , psc.EnglishProductSubcategoryName as Subcategory 
FROM           dimproduct                 p
LEFT SEMI JOIN dimproductsubcategory      psc
ON (p.ProductSubcategoryKey = psc.ProductSubcategoryKey)
LIMIT 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Cross Join
-- MAGIC 
-- MAGIC Returns a cartesian product of the two tables, ie. all posible combinations of column values.
-- MAGIC 
-- MAGIC Rarely useful but a good way to generate a valedation table such as a 
-- MAGIC general ledger key validation table. 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC In the example we want every combination of product category and subcategory.

-- COMMAND ----------

-- DBTITLE 1,Look at the product categories first
select EnglishProductCategoryName from  dimproductcategory

-- COMMAND ----------

-- DBTITLE 1,Cross Join - Cartesian Product
SELECT      c.EnglishProductCategoryName as Category, sc.EnglishProductSubcategoryName as Subcategory
FROM        dimproductcategory     c
CROSS JOIN  dimproductsubcategory  sc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Anti Join
-- MAGIC Returns values from the left side that has no match with the right. It is also referred to as a left anti join.

-- COMMAND ----------

-- DBTITLE 1,Anti Join
SELECT     p.ProductKey, p.EnglishProductName as Product, p.ProductSubcategoryKey
FROM       dimproduct                 p
ANTI JOIN  dimproductsubcategory      psc
ON (p.ProductSubcategoryKey = psc.ProductSubcategoryKey)
--WHERE psc.ProductSubcategoryKey IS NULL  -- This column is not in the results
LIMIT 3
