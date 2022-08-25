# Databricks notebook source
import pandas as pd
import json
import random

file_name = '/dbfs/FileStore/SAME/source2/open_ai_opinions.json'

with open(file_name, 'r', encoding='utf-8') as f:
    json_data = json.load(f)
    
data = pd.read_json(json_data)
data = data['opinions'].apply(pd.Series)

data.rename(columns = {'opinion':'content'}, inplace = True)

# Use predefined podcasts that are in sports-football category
podcasts_id_list = ['db76c79afe14521f4325fbffa1eecc49', 'fba9726f8c88415550bc878269fd3deb', 'a57a7ff82427bb573aefeafad9f03383', 'a14001bc5971e17c477d22a2f8ba8d87', 'a61cde11d99095d5f55c509e4729dbd1', 'a4bf93c145ee7d2b5ab9e2390791519b']

# Assign podcasts id randomly
random.seed(10)
data['podcast_id'] = random.choices(podcasts_id_list, k=data.shape[0])

# Set some other fake data
data['author_id'] = 'fake_user'
data['created_at'] = '2022-09-09T18:14:32-07:00'
data['title'] = 'Podcast review'

data = data[['podcast_id', 'title','content', 'rating', 'author_id', 'created_at']]

# COMMAND ----------

spark_data = spark.createDataFrame(data)

# COMMAND ----------

spark_data

# COMMAND ----------

table_name = 'reviews'

spark_data.coalesce(1).write.mode('append').format("com.databricks.spark.csv").option("header", "true").option("multiLine", "true").option("ignoreLeadingWhiteSpace", "true").option("sep","||").option("quoteMode", "ALL").option("parserLib", "univocity").save("/FileStore/SAME/source1/{}/{}.csv".format(table_name,table_name))

# COMMAND ----------

