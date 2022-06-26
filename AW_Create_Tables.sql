-- Databricks notebook source
-- MAGIC %md ## Author: Bryan Cafferky  Copyright 2/28/2021

-- COMMAND ----------

-- MAGIC %md # Create SQL Tables...

-- COMMAND ----------

-- MAGIC %md https://docs.databricks.com/getting-started/quick-start.html

-- COMMAND ----------

-- DBTITLE 1,Create a Database
CREATE DATABASE IF NOT EXISTS somjit_practice

-- COMMAND ----------

-- DBTITLE 1,Change Database Context
USE somjit_practice

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### New Syntax requirement - must use absolute file path, need to add / to beginning of path.

-- COMMAND ----------

-- DBTITLE 1,Create dimdate table
DROP TABLE IF EXISTS dimdate;

CREATE TABLE dimdate USING CSV 
OPTIONS (path "/FileStore/tables/DimDate.csv", header "true", inferSchema="true");

SELECT * FROM dimdate limit 2;

-- COMMAND ----------

-- DBTITLE 1,Create dimcustomer table
DROP TABLE IF EXISTS dimcustomer;

CREATE TABLE dimcustomer USING CSV OPTIONS (path "/FileStore/tables/DimCustomer.csv", header "true", inferSchema="true");

SELECT * FROM dimcustomer limit 2;

-- COMMAND ----------

-- DBTITLE 1,Create dimgeography table
DROP TABLE IF EXISTS dimgeography;

CREATE TABLE dimgeography USING CSV OPTIONS (path "/FileStore/tables/DimGeography.csv", header "true", inferSchema="true");

SELECT * FROM dimgeography limit 2;

-- COMMAND ----------

-- DBTITLE 1,Create dimproduct table
DROP TABLE IF EXISTS dimproduct;

CREATE TABLE dimproduct USING CSV OPTIONS (path "/FileStore/tables/DimProduct.csv", header "true", inferSchema="true");

SELECT * FROM dimproduct limit 2;

-- COMMAND ----------

-- DBTITLE 1,Create dimproductcategory table
DROP TABLE IF EXISTS dimproductcategory;

CREATE TABLE dimproductcategory USING CSV OPTIONS (path "/FileStore/tables/DimProductCategory.csv", header "true", inferSchema="true");

SELECT * FROM dimproductcategory limit 2;

-- COMMAND ----------

-- DBTITLE 1,Create dimproductsubcategory
DROP TABLE IF EXISTS dimproductsubcategory;

CREATE TABLE dimproductsubcategory USING CSV OPTIONS (path "/FileStore/tables/DimProductSubcategory.csv", header "true", inferSchema="true");

SELECT * FROM dimproductsubcategory limit 2;

-- COMMAND ----------

-- DBTITLE 1,Create dimsalesreason table
DROP TABLE IF EXISTS dimsalesreason;

CREATE TABLE dimsalesreason USING CSV OPTIONS (path "/FileStore/tables/DimSalesReason.csv", header "true", inferSchema="true");

SELECT * FROM dimsalesreason limit 2;

-- COMMAND ----------

-- DBTITLE 1,Create dimsalesterritory table
DROP TABLE IF EXISTS dimsalesterritory;

CREATE TABLE dimsalesterritory USING CSV OPTIONS (path "/FileStore/tables/DimSalesTerritory.csv", header "true", inferSchema="true");

SELECT * FROM dimsalesterritory limit 2;

-- COMMAND ----------

-- DBTITLE 1,Create factinternetsales table
DROP TABLE IF EXISTS factinternetsales;

CREATE TABLE factinternetsales USING CSV OPTIONS (path "/FileStore/tables/FactInternetSales.csv", header "true", inferSchema="true");

SELECT * FROM factinternetsales limit 2;

-- COMMAND ----------

-- DBTITLE 1,Create factinternetsalesreasontable defining column names and types
DROP TABLE IF EXISTS factinternetsalesreason;

CREATE TABLE factinternetsalesreason (SalesOrderNumber string, SalesOrderLineNumber int, SalesReasonKey int)
USING CSV OPTIONS (path "/FileStore/tables/FactInternetSalesReason.csv", header "false", inferSchema="false");

SELECT * FROM factinternetsalesreason limit 5;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED factinternetsalesreason

-- COMMAND ----------

-- DBTITLE 1,List the tables
SHOW TABLES

-- COMMAND ----------

SELECT a.SalesOrderNumber, b.EnglishProductName
FROM 
factinternetsales a 
INNER JOIN 
dimproduct b
ON a.ProductKey = b.ProductKey
LIMIT 20;
