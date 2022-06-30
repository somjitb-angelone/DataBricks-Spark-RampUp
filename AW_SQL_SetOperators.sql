-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Notebook Name: AW_SQL_SetOperators
-- MAGIC #### Author: Bryan Cafferky, BPC Global Solutions LLC
-- MAGIC ### 
-- MAGIC #### Purpose:  Demonstrate using Spark SQL Set Operators.  Not for production use.
-- MAGIC ####           Just intended to explain the concepts.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using Spark SQL Set Operators:
-- MAGIC 
-- MAGIC - UNION and UNION ALL
-- MAGIC - UNION DISTINCT
-- MAGIC - INTERSECT
-- MAGIC - EXCEPT or Minus

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC #### Operator descriptions copied from Databricks documentation at:
-- MAGIC 
-- MAGIC https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select-setops.html

-- COMMAND ----------

USE somjit_practice

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DROP TABLE IF EXISTS productlist1;

CREATE TABLE productlist1 as
SELECT ProductKey, p.EnglishProductName as Product, psc.EnglishProductSubcategoryName as Subcategory
FROM       dimproduct            p
INNER JOIN dimproductsubcategory psc
ON (p.ProductSubcategoryKey = psc.ProductSubcategoryKey)
WHERE ProductKey in (210, 211, 212, 215);

-- COMMAND ----------

DESCRIBE TABLE EXTENDED productlist1

-- COMMAND ----------

DROP TABLE IF EXISTS productlist2;

CREATE TABLE productlist2 as
SELECT ProductKey, p.EnglishProductName as Product, psc.EnglishProductSubcategoryName as Subcategory
FROM       dimproduct            p
INNER JOIN dimproductsubcategory psc
ON (p.ProductSubcategoryKey = psc.ProductSubcategoryKey)
WHERE ProductKey in (210, 215, 222, 224);

-- COMMAND ----------

-- MAGIC %md ### Here are both tables joined to see how they compare.

-- COMMAND ----------

SELECT * 
FROM            productlist1 p1
FULL OUTER JOIN productlist2 p2
on (p1.ProductKey = p2.ProductKey)
ORDER BY ifnull(p1.ProductKey,9999) 

-- COMMAND ----------

-- MAGIC %md ### UNION / UNION DISTINCT/ UNION ALL
-- MAGIC 
-- MAGIC UNION and UNION ALL return the rows that are found in either relation. UNION (alternatively, UNION DISTINCT) takes only distinct rows while UNION ALL does not remove duplicates from the result rows.

-- COMMAND ----------

-- MAGIC %md #### UNION 

-- COMMAND ----------

-- ProductKey 210 and 215 are in both tables
-- but they will only be returned once, i.e. duplicates are removed.
-- Results are not sorted.
SELECT * FROM productlist1
UNION 
SELECT * FROM productlist2;

-- COMMAND ----------

-- MAGIC %md #### UNION DISTINCT

-- COMMAND ----------

-- ProductKey 210 and 215 are in both tables
-- but they will only be returned once, i.e. duplicates are removed.
SELECT * FROM productlist1
UNION DISTINCT
SELECT * FROM productlist2;

-- COMMAND ----------

-- MAGIC %md #### UNION ALL

-- COMMAND ----------

-- ProductKey 210 and 215 are in both tables
-- but they will only be returned once, i.e. duplicates are NOT removed.
-- Results are NOT sorted.  We added a sort so we can see the duplicates.
SELECT * FROM productlist1
UNION ALL
SELECT * FROM productlist2
ORDER BY ProductKey

-- COMMAND ----------

-- MAGIC %md ### INTERSECT / INTERSECT DISTICT / INTERSECT ALL
-- MAGIC 
-- MAGIC INTERSECT and INTERSECT ALL return the rows from the first query that are also found in the second. INTERSECT (alternatively, INTERSECT DISTINCT) takes only distinct rows while INTERSECT ALL does not remove duplicates from the result rows.

-- COMMAND ----------

SELECT * 
FROM            productlist1 p1
FULL OUTER JOIN productlist2 p2
on (p1.ProductKey = p2.ProductKey)
ORDER BY ifnull(p1.ProductKey,9999) 

-- COMMAND ----------

-- MAGIC %md #### INTERSECT

-- COMMAND ----------

-- ProductKey 210 and 215 are in both tables
-- Only the duplicates, i.e. rows in the first query found in the second are returned.
SELECT * FROM productlist1
INTERSECT
SELECT * FROM productlist2

-- COMMAND ----------

-- MAGIC %md #### INTERSECT DISTINCT

-- COMMAND ----------

-- ProductKey 210 and 215 are in both tables
-- Only the duplicates, i.e. rows in both tables will be returned.
SELECT * FROM productlist1
INTERSECT DISTINCT
SELECT * FROM productlist2

-- COMMAND ----------

-- MAGIC %md #### INTERSECT ALL

-- COMMAND ----------

-- ProductKey 210 and 215 are in both tables
-- Only the duplicates, i.e. rows in both tables will be returned.
SELECT * FROM productlist1
INTERSECT ALL
SELECT * FROM productlist2

-- COMMAND ----------

SELECT * FROM VALUES (1), (2), (3), (3), (4), (4)
INTERSECT ALL 
SELECT * FROM VALUES (2), (3), (5), (6)
--SELECT * FROM VALUES (2), (3), (3), (5), (6)

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md ### EXCEPT / EXCEPT DISTINCT / EXCEPT ALL  (Minus is an alias for EXCEPT)
-- MAGIC 
-- MAGIC EXCEPT and EXCEPT ALL return the rows that are found in the first query but not the second. EXCEPT (alternatively, EXCEPT DISTINCT) takes only distinct rows while EXCEPT ALL does not remove duplicates from the result rows. Note that MINUS is an alias for EXCEPT.

-- COMMAND ----------

SELECT * 
FROM            productlist1 p1
FULL OUTER JOIN productlist2 p2
on (p1.ProductKey = p2.ProductKey)
ORDER BY ifnull(p1.ProductKey,9999) 

-- COMMAND ----------

-- MAGIC %md #### EXCEPT (alias MINUS)

-- COMMAND ----------

-- ProductKey 210 and 215 are in both tables
-- Only retruns the rows found in first query but not second.
SELECT * FROM productlist1
EXCEPT 
SELECT * FROM productlist2

-- COMMAND ----------

-- MAGIC %md #### EXCEPT DISTINCT

-- COMMAND ----------

-- ProductKey 210 and 215 are in both tables
-- Only retruns the rows found in first query but not second.
SELECT * FROM productlist1
EXCEPT DISTINCT
SELECT * FROM productlist2

-- COMMAND ----------

-- MAGIC %md #### EXCEPT ALL

-- COMMAND ----------

-- ProductKey 210 and 215 are in both tables
-- Only retruns the rows found in first query but not second.
SELECT * FROM productlist1
EXCEPT ALL 
SELECT * FROM productlist2

-- COMMAND ----------

-- MAGIC %md ### Using EXCEPT ALL - 2nd example

-- COMMAND ----------

SELECT * FROM VALUES (1), (2), (3), (3), (4), (4), (9), (9)
EXCEPT ALL
SELECT * FROM VALUES (2), (3), (5), (6)
