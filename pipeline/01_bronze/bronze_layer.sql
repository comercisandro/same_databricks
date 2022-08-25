-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Tables creation of Bronze layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Table name: 
-- MAGIC 
-- MAGIC bronze_categories
-- MAGIC 
-- MAGIC ### Description
-- MAGIC Saves the category of every one of the podcast. Each podcast can have many categories associated so it could appear more than once.
-- MAGIC 
-- MAGIC ### Structure 
-- MAGIC 
-- MAGIC - __podcast_id (STRING)__: An unique identifier for a podcast.
-- MAGIC - __category (STRING)__: Category associated with the podcast.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_categories
COMMENT "The raw information for the categories."
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM cloud_files("/FileStore/SAME/source1/categories", "csv", map("delimiter", "||", "header", "true", "schema", "podcast_id STRING, category STRING"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Table name: 
-- MAGIC 
-- MAGIC bronze_reviews
-- MAGIC 
-- MAGIC ### Description
-- MAGIC 
-- MAGIC Stores information about the ratings and reviews made by the users about different podcasts.
-- MAGIC 
-- MAGIC ### Structure 
-- MAGIC 
-- MAGIC - __podcast_id (STRING)__: An unique identifier for a podcast.
-- MAGIC - __title (STRING)__: Title for the review. Works like a short summary of the long review.
-- MAGIC - __content (STRING)__: The long review about the podcast.
-- MAGIC - __rating (INT)__: The rating for the podcast given by the user.
-- MAGIC - __author_id (STRING)__: An unique ID for the author of the review (user).
-- MAGIC - __created_at (STRING)__: Timestamp when the review was created.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_reviews
COMMENT "The raw information about the reviews."
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM cloud_files("/FileStore/SAME/source1/reviews", "csv", map("delimiter", "||", "header", "true", "schema", "podcast_id STRING, title STRING, content STRING, rating INT, author_id STRING, created_at STRING"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Table name: 
-- MAGIC 
-- MAGIC bronze_podcasts
-- MAGIC 
-- MAGIC ### Description
-- MAGIC 
-- MAGIC Stores information about the podcast like url in iTunes and name of the podcast.
-- MAGIC 
-- MAGIC ### Structure 
-- MAGIC 
-- MAGIC - __podcast_id (STRING)__: An unique identifier for a podcast.
-- MAGIC - __itunes_id (STRING)__: ID of the podcast in iTunes.
-- MAGIC - __slug (STRING)__: Slug of the podcast.
-- MAGIC - __itunes_url (STRING)__: URL of the podcast in iTunes.
-- MAGIC - __title (STRING)__: The podcast name.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_podcasts
COMMENT "The raw information about the podcasts."
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM cloud_files("/FileStore/SAME/source1/podcasts", "csv", map("delimiter", "||", "header", "true", "schema", "podcast_id STRING, itunes_id STRING, slug STRING, itunes_url STRING, title STRING"))