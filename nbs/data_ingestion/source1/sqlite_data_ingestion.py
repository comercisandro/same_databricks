# Databricks notebook source
# MAGIC %sh
# MAGIC unzip /dbfs/FileStore/SAME/source1/archive.zip -d /dbfs/FileStore/SAME/source1

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def get_dataframe_from_sqlite_db(table_name):
# MAGIC     
# MAGIC     df = sqlContext.read.format('jdbc').\
# MAGIC          options(url='jdbc:sqlite:/dbfs/FileStore/SAME/source1/database.sqlite',\
# MAGIC          dbtable=table_name,driver='org.sqlite.JDBC').load()
# MAGIC 
# MAGIC                                            
# MAGIC     return(df)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import regexp_replace, col
# MAGIC 
# MAGIC # This cell will fetch data from every table in sqlite database, load it to a dataframe and the save it to Databricks FS, not to OS FS.
# MAGIC # If the files already exists it will prompt an error.
# MAGIC 
# MAGIC table_names = ["categories", "reviews", "podcasts"]
# MAGIC 
# MAGIC 
# MAGIC for table_name in table_names:
# MAGIC     
# MAGIC     df = get_dataframe_from_sqlite_db(table_name)
# MAGIC     
# MAGIC     if table_name == 'reviews':
# MAGIC         
# MAGIC         df = df.withColumn("content", regexp_replace(col("content"), "[\n\r]", " "))
# MAGIC     
# MAGIC     #output_path = "/FileStore/SAME/source1/{}".format(table_name)
# MAGIC     #df.write.mode('append').parquet(output_path)
# MAGIC     df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("multiLine", "true").option("ignoreLeadingWhiteSpace", "true").option("sep","||").option("quoteMode", "ALL").option("parserLib", "univocity").save("/FileStore/SAME/source1/{}/{}.csv".format(table_name,table_name))

# COMMAND ----------

