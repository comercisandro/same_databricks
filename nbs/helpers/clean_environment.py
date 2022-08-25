# Databricks notebook source
dbutils.fs.rm("dbfs:/FileStore/SAME/source1/reviews", True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/SAME/source1/podcasts", True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/SAME/source1/categories", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from same_db.bronze_reviews;
# MAGIC 
# MAGIC drop table same_db.bronze_reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from same_db.bronze_categories;
# MAGIC 
# MAGIC drop table same_db.bronze_categories;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from same_db.bronze_podcasts;
# MAGIC 
# MAGIC drop table same_db.bronze_podcasts;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from same_db.silver_podcasts;
# MAGIC 
# MAGIC drop table same_db.silver_podcasts;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from same_db.silver_categories;
# MAGIC 
# MAGIC drop table same_db.silver_categories;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM same_db.bronze_categories;
# MAGIC 
# MAGIC VACUUM same_db.bronze_podcasts;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM same_db.bronze_reviews;

# COMMAND ----------


