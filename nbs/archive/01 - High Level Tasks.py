# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration 

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/SAME/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /dbfs/FileStore/SAME/archive.zip -d /dbfs/FileStore/SAME/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /dbfs/FileStore/SAME/

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def get_dataframe_from_sqlite_db(table_name):
# MAGIC     
# MAGIC     df = sqlContext.read.format('jdbc').\
# MAGIC          options(url='jdbc:sqlite:/dbfs/FileStore/SAME/database.sqlite',\
# MAGIC          dbtable=table_name,driver='org.sqlite.JDBC').load()
# MAGIC 
# MAGIC     return(df)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # This cell will fetch data from every table in sqlite database, load it to a dataframe and the save it to Databricks FS, not to OS FS.
# MAGIC # If the files already exists it will prompt an error.
# MAGIC 
# MAGIC table_names = ["podcasts", "categories", "runs", "reviews"]
# MAGIC 
# MAGIC for table_name in table_names:
# MAGIC     
# MAGIC     df = get_dataframe_from_sqlite_db(table_name)
# MAGIC     
# MAGIC     df.write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/dbfs/FileStore/SAME/{}".format(table_name))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Run this cell to remove files from Databricks File System (For common OS file removal you would use rm). 
# MAGIC # Folders should be empty to be removed.
# MAGIC 
# MAGIC for table_name in table_names:
# MAGIC     
# MAGIC     dbutils.fs.rm("/dbfs/FileStore/SAME/{}".format(table_name))

# COMMAND ----------

files = dbutils.fs.ls(f"/dbfs/FileStore/SAME") # List all the files
display(files)

# COMMAND ----------

# MAGIC %python
# MAGIC # *** TO BE IMPLEMENTED *** ALREADY IMPLEMENTED 
# MAGIC 
# MAGIC # Split files in batches. Simulate new data.
# MAGIC # Add noise to data.
# MAGIC # Move data in dbfs to Bronze Layer as delta table.
# MAGIC 
# MAGIC #read_format = 'delta'
# MAGIC 
# MAGIC ##################### READ FILES FROM DATABASE #################
# MAGIC import sqlite3
# MAGIC import pandas as pd
# MAGIC cnx = sqlite3.connect('/dbfs/FileStore/SAME/database.sqlite')
# MAGIC df_podcasts = pd.read_sql_query("SELECT * FROM podcasts", cnx)
# MAGIC df_categories = pd.read_sql_query("SELECT * FROM categories", cnx)
# MAGIC df_runs = pd.read_sql_query("SELECT * FROM runs", cnx)
# MAGIC df_reviews = pd.read_sql_query("SELECT * FROM reviews", cnx)
# MAGIC ################################################################
# MAGIC 
# MAGIC ############### Create one .csv file for each table ############
# MAGIC #df_podcasts.to_csv("/dbfs/FileStore/SAME/podcasts.csv")
# MAGIC #df_categories.to_csv("/dbfs/FileStore/SAME/categories.csv")
# MAGIC #df_runs.to_csv("/dbfs/FileStore/SAME/runs.csv")
# MAGIC #df_reviews.to_csv("/dbfs/FileStore/SAME/reviews.csv")

# COMMAND ----------

df_podcasts.head()

# COMMAND ----------

df_categories.head()

# COMMAND ----------

df_runs.head()

# COMMAND ----------

df_reviews.head()

# COMMAND ----------

#!ls /dbfs/FileStore/SAME
import numpy as np

############### Ensuciar los datos #############################

# Replace 15% of Rows by None
df1 = df_podcasts
df1.loc[df1.sample(int(len(df1) * .15)).index, 'title'] = None
#print(df1[df1['title'].isnull()].shape[0])

df2 = df_reviews
df2.loc[df2.sample(int(len(df2) * .15)).index, 'rating'] = None
#print(df2[df2['rating'].isnull()].shape[0])

# Change the strucutre of 15% of the dates
############ NOTE ################
# There are two rows, index 1136903 and 1136904, which have symbols in the column "created_at",
# so I eliminated them with the following command:
df2 = df2.drop(index=[1136903,1136904])

df2['created_at']= pd.to_datetime(df2.created_at, format='%Y-%m-%d')
np.random.seed(1)
index_list=np.random.randint(0, len(df2), size=int(len(df2) * .15))
#print(index_list)
df2['created_at'] = np.where(df2.index.isin(index_list),df2.created_at.dt.strftime('%d/%m/%y'),df2['created_at'])

# COMMAND ----------

# Split df_reviews by month and save them as .csv files in different folders

df2['year_month'] = pd.to_datetime(df_reviews.created_at)
df2['year_month'] = df2['year_month'].dt.strftime('%Y%m')
year_month_list = list(df2['year_month'].unique())
year_month_list.sort()

import os

for y_m in year_month_list:
    reviews_df_month = df2.loc[df2['year_month']== y_m]
    outname = f"""reviews_{y_m}.csv"""
    outdir = f"""/dbfs/FileStore/SAME/batch_{y_m}"""
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    fullname = os.path.join(outdir, outname)    
    reviews_df_month.to_csv(fullname)
    
    podcast_df2 = df1.loc[df1['podcast_id'].isin(list(reviews_df_month['podcast_id']))]
    podcast_df2.to_csv(f"""/dbfs/FileStore/SAME/batch_{y_m}/podcast_{y_m}.csv""")
                                                               
    categories_df2 = df_categories.loc[df_categories['podcast_id'].isin(list(reviews_df_month['podcast_id']))]
    categories_df2.to_csv(f"""/dbfs/FileStore/SAME/batch_{y_m}/categories_{y_m}.csv""")

# COMMAND ----------

#!mkdir /dbfs/FileStore/SAME/batch_200803
!ls -l /dbfs/FileStore/SAME/source1/

# COMMAND ----------

# 1) Create database (bronze, silver and gold)
spark.sql("CREATE DATABASE IF NOT EXISTS %s".format("same_bronze_db"));
spark.sql("CREATE DATABASE IF NOT EXISTS %s".format("same_silver_db"));
spark.sql("CREATE DATABASE IF NOT EXISTS %s".format("same_gold_db"));

# 2) Create tables in each db
##### BRONZE
{ { [CREATE OR REPLACE TABLE] | CREATE TABLE [ IF NOT EXISTS ] }
  "same_bronze_db"."podcasts"
  [ column_definition ] [ USING data_source ]
  [ table_clauses ]
  [ AS query ] }

column_definition
  ( { podcast_id str [ NOT NULL ]
      [ GENERATED ALWAYS AS ( expres ) ] [ COMMENT " this is the podcast_id" ] }
   { itunes_id int [ NOT NULL ]
      [ GENERATED ALWAYS AS ( expres ) ] [ COMMENT " this is the itunes_id" ] }
   { slug str [ NOT NULL ]
      [ GENERATED ALWAYS AS ( expres ) ] [ COMMENT " this is the slug" ] }
     itunes_url str [ NOT NULL ]
      [ GENERATED ALWAYS AS ( expres ) ] [ COMMENT " this is the slug" ] }
     title str [ NOT NULL ]
      [ GENERATED ALWAYS AS ( expres ) ] [ COMMENT " this is the slug" ] }[, ...] )

table_clauses
  { OPTIONS ( { option_key [ = ] option_value } ) |
    PARTITIONED BY 'year_month' |
    #clustered_by_clause |
    LOCATION /DatabaseTables/same_bronze_db |
    COMMENT "bronze_podcast table" |
    TBLPROPERTIES ( {"quality"  =  "bronze" } ) }

#clustered_by_clause
#  { CLUSTERED BY ( cluster_column [, ...] )
#    [ SORTED BY ( { sortColumn [ ASC | DESC ] } [, ...] ) ]
#    INTO num_buckets BUCKETS }


# Move from raw to bronze (Use AutoLoader)

##### SILVER

{ { [CREATE OR REPLACE TABLE] | CREATE TABLE [ IF NOT EXISTS ] }
  "same_silver_db"."categories_podcasts"
  [ column_definition ] [ USING data_source ]
  [ table_clauses ]
  [ AS query ] }

column_definition
  ( { podcast_id int [ NOT NULL ]
      [ GENERATED ALWAYS AS ( expres ) ] [ COMMENT " this is the podcast_id" ] } [, ...] )

table_clauses
  { OPTIONS ( { option_key [ = ] option_value } ) |
    PARTITIONED BY 'year_month' |
    #clustered_by_clause |
    LOCATION /DatabaseTables/same_bronze_db |
    COMMENT "bronze_podcast table" |
    TBLPROPERTIES ( {"quality"  =  "silver" } ) }
    
##### GOLD

{ { [CREATE OR REPLACE TABLE] | CREATE TABLE [ IF NOT EXISTS ] }
  "same_gold_db"."categories_podcasts_gold"
  [ column_definition ] [ USING data_source ]
  [ table_clauses ]
  [ AS query ] }

column_definition
  ( { podcast_id int [ NOT NULL ]
      [ GENERATED ALWAYS AS ( expres ) ] [ COMMENT " this is the podcast_id" ] } [, ...] )

table_clauses
  { OPTIONS ( { option_key [ = ] option_value } ) |
    PARTITIONED BY 'year_month' |
    #clustered_by_clause |
    LOCATION /DatabaseTables/same_bronze_db |
    COMMENT "bronze_podcast table" |
    TBLPROPERTIES ( {"quality"  =  "gold" } ) }

# COMMAND ----------

# Move from bronze to silver

df = spark.sql('SELECT * FROM "same_bronze_db"."podcasts" ')
df_clean = prep_data(df)
def prep_data(df):
    return df_clean

# Write data to silver table

reviews
podcasts
categories

categories_podcast



MERGE INTO "same_silver_db"."categories_podcasts" [target_alias]
   USING source_table_reference [source_alias]
   ON merge_condition
   [ WHEN MATCHED [ AND condition ] THEN matched_action ] [...]
   [ WHEN NOT MATCHED [ AND condition ]  THEN not_matched_action ] [...]

# Or use the dataframe API

#To see if it worked:
query1 = SELECT category, COUNT(DISTINCT podcast_id) FROM "same_silver_db"."categories_podcasts"
GROUP BY category
ORDER BY category


# Write data to gold table
MERGE INTO "same_gold_db"."categories_podcasts_gold" [target_alias]
   USING query1 [source_alias]
   ON merge_condition
   [ WHEN MATCHED [ AND condition ] THEN matched_action ] [...]
   [ WHEN NOT MATCHED [ AND condition ]  THEN not_matched_action ] [...]

# Or use the dataframe API

#To see if it worked:
SELECT * FROM "same_silver_db"."categories_podcasts"

# COMMAND ----------

# Read from gold table and create dashboards
# Put this inside the pipeline


# COMMAND ----------

################# Build function to ingest new batches of data given a schedule.  #############################
# TO DO

spark.sql("CREATE DATABASE IF NOT EXISTS %s".format(hiveDatabase));

CREATE TABLE live_podcasts_table
    TBLPROPERTIES("quality" = "bronze")
AS SELECT * FROM cloud_files('/dbfs/FileStore/SAME/batch_*/podcast_*' , 'csv')

'''for y_m in year_month_list:
    files = dbutils.fs.ls(f"/dbfs/FileStore/SAME/batch_{y_m}") # List all the files
    display(files)                                             # Display the list of files

    df2 = spark.readStream.format("cloudFiles")\
                 .option("cloudFiles.format", "json")\
                 .option("header", True)\
                 .load(/dbfs/FileStore/SAME/batch_{y_m})
    (df2.writeStream.format("delta")
     .trigger(once=True)
     .option("checkpointLocation", chckpt_path)
     .table("a Delta Table Name"))
    

df2_filenametime = df2.withColumn("ingest_file_name", F.input_file_name()).withColumn("ingested_at", F.current_timestamp())

def writeDeltaTable(aDataFrame, aTableName):
    aDataFrame.repartition(int(spark.conf.get("spark.sql.shuffle.partitions"))).cache()
    aDataFrame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(aTableName)
    
writeDeltaTable(aDataFrame, aTableName)
    
spark.sql("CREATE DATABASE IF NOT EXISTS %s".format(hiveDatabase)); '''

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # *** TO BE IMPLEMENTED ***
# MAGIC 
# MAGIC # Run Data Validation with Great Expectations.
# MAGIC # Read data from Delta tables in Bronze Layer.
# MAGIC # Check new changes with time travel, for example if new data is upserted.
# MAGIC 
# MAGIC df = spark.sql("SELECT * FROM podcasts LIMIT 20")
# MAGIC display(df)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # *** TO BE IMPLEMENTED ***
# MAGIC 
# MAGIC # Write functions to clean the data.
# MAGIC # Clean data. Use pipelines to run the notebook with data cleansing process. 
# MAGIC # Bronze -> Silver: Move data from Bronze Layer to Silver Layer.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # *** TO BE IMPLEMENTED ***
# MAGIC 
# MAGIC # Write functions to join and aggregate from cleansed tables.
# MAGIC # Silver -> Gold. Move data from Silver Layer to Gold Layer.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # *** TO BE IMPLEMENTED ***
# MAGIC 
# MAGIC # Implement Databricks Dashboards with tables in Gold Layer.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # *** TO BE IMPLEMENTED ***
# MAGIC 
# MAGIC # Build the whole pipelines with workflows and schedules.

# COMMAND ----------

df = spark.sql("SELECT * FROM categories LIMIT 20")
display(df)

# COMMAND ----------

df = spark.sql("""SELECT 
                      b.category, 
                      COUNT(DISTINCT a.podcast_id) podcast_count, 
                      ROUND(AVG(rating),2) AS avg_rating,
                      PERCENTILE_APPROX(rating, 0.5) AS median_rating
                  FROM reviews a 
                  JOIN categories b
                  ON a.podcast_id=b.podcast_id
                  GROUP BY b.category""")
display(df)

# COMMAND ----------

df = spark.sql("""SELECT 
                      YEAR(created_at)*100 + MONTH(created_at) AS period,
                      COUNT(DISTINCT podcast_id) as podcast_count
                  FROM reviews
                  GROUP BY YEAR(created_at)*100 + MONTH(created_at)
                  ORDER BY YEAR(created_at)*100 + MONTH(created_at)""")
display(df)

# COMMAND ----------

def inject_wrong_format_dates(df, percentage, column_name):
    
    set.seed(1)
    df
    
2018-04-24T12:05:16-07:00
%Y-%m-%d

%m-%y-%d

df.replace()

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("dbfs:/dbfs/FileStore/SAME/{}")

df.registerTempTable("podcast")