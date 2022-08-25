# Databricks notebook source
# MAGIC %md
# MAGIC # Background
# MAGIC 
# MAGIC In today’s world, every enterprise receives data from multiple sources (internal and external) in variety of formats. More than often, the data is not in standard formats due to the varying standards followed at the source systems. It is very common that the team managing datalakes need to cleanse, enhance, and standardize the data via a series of ETL tools and frameworks. Databricks provides many capabilities out of the box to create and manage data engineering pipelines, store data in its lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Problem Statement
# MAGIC As a data engineer, it’s your team’s responsibility to build and manage data engineering pipelines using Databricks. Build data pipelines which can provide the following functionalities (how complex and deep you want to build is upto your imagination. Below is just a suggested list).
# MAGIC 1. Follow Databricks Medallion architecture to build a data lakehouse.
# MAGIC 2. Pipelines should be able to
# MAGIC   - Validate incoming file’s schema
# MAGIC   - Validate data fields for </br>
# MAGIC     - Nulls </br>
# MAGIC     - Completeness </br>
# MAGIC     - Field types </br>
# MAGIC     - Length check </br>
# MAGIC     - Date/Timestamp formats etc., </br>
# MAGIC   - Transform/Standardize date fields </br>
# MAGIC   - Derive new fields from existing fields </br>
# MAGIC   - Aggregate data for consumption
# MAGIC 3. Delta tables for storing data in Silver/Gold layers
# MAGIC 4. Use Delta Live Tables (DLT) to build pipelines and note down the Pro’s and Con’s.
# MAGIC 5. Incorporate error handling, check logs for job failures
# MAGIC 6. Create jobs, workflows etc.,
# MAGIC 7. Anything else you can think of to enrich your solution, including:</br>
# MAGIC   - Visualizing your Delta Live Tables via Databricks SQL Dashboards.</br>
# MAGIC   - Adding version control to your code via Databricks Repos.

# COMMAND ----------

# MAGIC %md
# MAGIC # Data
# MAGIC 
# MAGIC 1. Bring any publicly available data for the purpose of this hackathon. Some sample data sources are
# MAGIC - [https://www.kaggle.com/datasets/shekpaul/global-superstore](https://www.kaggle.com/datasets/shekpaul/global-superstore)
# MAGIC - [https://www.ncdc.noaa.gov/cdo-web/datasets](https://www.ncdc.noaa.gov/cdo-web/datasets)
# MAGIC - [https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
# MAGIC - [https://data.gov/](https://data.gov/)
# MAGIC 2. Upload your data into Databricks DBFS. Depending on your use case, it may be appropriate to create a process which incrementally copies subsets of your data to a folder from where it will be consumed by your DE process.
# MAGIC 3. Restrict your dataset size to less than 1GB as the goal of this hackathon is to test the functionality than performance
# MAGIC 4. Simulate data inconsistencies to cover your use cases as most likely the public data might be clean

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Team Tasks
# MAGIC 
# MAGIC ## First part:
# MAGIC  
# MAGIC - Split the file into different chunks so we can simulate batches of data coming into the bronze layer. __(Ana) DONE__
# MAGIC - Build function to ingest new batches of data given a schedule. __(Ana)__
# MAGIC - Build a function to inject corrupted dates or with a different format for further treatment. A parameter could be percentage of invalid dates.__(Ana) DONE__
# MAGIC - Build a function to inject null values. A parameter could be percentage of null values in a particular column. __(Ana) DONE__
# MAGIC - Check Databricks Repo. __(Sandro)__
# MAGIC - Keep ETL folder structure organized [link1](https://www.reddit.com/r/ETL/comments/82kovh/comment/dvbpvym/?utm_source=reddit&utm_medium=web2x&context=3) __(Martin)__
# MAGIC - Check Databricks Pipelines. __(Sandro)__
# MAGIC - Check Databricks Dashboard __(Evelyn)__.
# MAGIC - Data Validation: Check Great Expectations library (supports SparkDF). [link1](https://www.youtube.com/watch?v=1v0TZAsHa8c) [link2](https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_in_databricks/) __(Martin)__
# MAGIC - Create notebook to send files from DBFS to Bronze. __(Martin)__
# MAGIC 
# MAGIC ## Second part: 
# MAGIC Once raw is prepared.
# MAGIC - Identify tasks to be treated in each layer flow. Eg. Missing values treatment from Bronze to Silver, schema validation. Aggregations from Silver to Gold.
# MAGIC - Build pipeline to move data batches and with nulls/wrong date formats to Bronze Layer. Etc.
# MAGIC  

# COMMAND ----------

