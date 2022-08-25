# Databricks notebook source
input_data_path = "/FileStore/SAME/reviews.csv"

df = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").load(input_data_path))


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE same_bronze_db;
# MAGIC 
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE reviews
# MAGIC AS SELECT *
# MAGIC   FROM cloud_files(
# MAGIC     "/FileStore/SAME/reviews.csv",
# MAGIC     "csv",
# MAGIC     map("schema", "podcast_id STRING, title STRING, content STRING, rating INT, author_id STRING, created_at TIMESTAMP")
# MAGIC   )

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/SAME

# COMMAND ----------

# MAGIC %sh
# MAGIC rm /dbfs/FileStore/SAME/runs.csv

# COMMAND ----------

input_data_path = "/FileStore/SAME/batch_200512/categories_200512.csv"

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")).option("cloudFiles.format", "csv").load(input_data_path) 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE same_bronze_db;
# MAGIC 
# MAGIC LOAD DATA LOCAL INPATH "/FileStore/SAME/batch_200512/reviews_200512.csv" OVERWRITE INTO TABLE reviews;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC USE same_bronze_db;
# MAGIC 
# MAGIC CREATE OR REFRESH LIVE TABLE reviews
# MAGIC COMMENT "The raw reviews file."
# MAGIC AS SELECT * FROM csv.`/FileStore/SAME/reviews`;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC use same_bronze_db;
# MAGIC SELECT * FROM reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE same_bronze_db;
# MAGIC LOAD DATA LOCAL INPATH '/FileStore/SAME/reviews.csv' OVERWRITE INTO TABLE reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS same_bronze_db.reviews (podcast_id STRING, title STRING, content STRING, rating INT, author_id STRING, created_at TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE same_bronze_db;
# MAGIC 
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE reviews
# MAGIC AS SELECT *
# MAGIC   FROM cloud_files(
# MAGIC     "/FileStore/SAME/reviews.csv",
# MAGIC     "csv",
# MAGIC     map("schema", "podcast_id STRING, title STRING, content STRING, rating INT, author_id STRING, created_at TIMESTAMP")
# MAGIC   )

# COMMAND ----------

