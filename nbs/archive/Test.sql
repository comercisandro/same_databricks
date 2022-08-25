-- Databricks notebook source
CREATE STREAMING LIVE TABLE same_bronze_db.categories(
  CONSTRAINT valid_podcast_id EXPECT (podcast_id IS NOT NULL)
  )
COMMENT "The raw categories file."
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM cloud_files("/FileStore/SAME/categories.csv", "csv", map("delimiter", ",", "header", "true", "schema", "podcast_id STRING, category STRING"))

-- COMMAND ----------

select * from same_db.bronze_podcasts limit 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df = spark.read.csv('dbfs:/FileStore/SAME/batch_200512/categories_200512.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df.show()

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE same_silver_db.categories
AS SELECT * FROM STREAM(LIVE.same_bronze_db.categories) 

-- COMMAND ----------

USE same_bronze_db;

APPLY CHANGES INTO categories

-- COMMAND ----------

USE same_bronze_db;

select * from live.categories;

-- COMMAND ----------



-- COMMAND ----------

