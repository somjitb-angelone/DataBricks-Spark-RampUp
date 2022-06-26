-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Demonstrate use of SQL Data Definition Language (DDL) Statements
-- MAGIC Databricks Documentation: https://docs.databricks.com/spark/latest/spark-sql/index.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Use SQL Data Definition Language (DDL)

-- COMMAND ----------

CREATE DATABASE mydb;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE mydb;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED mydb;

-- COMMAND ----------

ALTER DATABASE mydb SET DBPROPERTIES (Production=true)

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED mydb

-- COMMAND ----------

DESCRIBE TABLE mytable;

-- COMMAND ----------

CREATE TABLE mytable (name STRING, age int, city STRING)
TBLPROPERTIES ('created.by.user' = 'Bryan', 'created.date' = '02-21-2021');

-- COMMAND ----------

SHOW TBLPROPERTIES mytable;

-- COMMAND ----------

SHOW TBLPROPERTIES mytable (created.by.user);

-- COMMAND ----------

DESCRIBE TABLE EXTENDED mytable;

-- COMMAND ----------

ALTER TABLE mytable RENAME TO mytable2;

-- COMMAND ----------

ALTER TABLE mytable ADD columns (title string, DOB timestamp);

-- COMMAND ----------

DESCRIBE TABLE mytable;

-- COMMAND ----------

ALTER TABLE mytable  ALTER COLUMN city COMMENT "address city";

-- COMMAND ----------

DESCRIBE TABLE mytable;

-- COMMAND ----------

ALTER TABLE mytable SET TBLPROPERTIES (sensitivedata=false)

-- COMMAND ----------

DESCRIBE TABLE EXTENDED mytable

-- COMMAND ----------

SHOW PARTITIONS mytable;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

SHOW TBLPROPERTIES mytable;

-- COMMAND ----------

SHOW CREATE TABLE mytable

-- COMMAND ----------

SHOW FUNCTIONS

-- COMMAND ----------

USE SCHEMA mydb;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up database objects...

-- COMMAND ----------

USE mydb;

SHOW TABLES;

-- COMMAND ----------

DROP DATABASE IF EXISTS mydb;

-- COMMAND ----------

DROP TABLE IF EXISTS mytable;

-- COMMAND ----------

CREATE DATABASE somjit_practice;

-- COMMAND ----------

-- MAGIC %sh python3 -V
