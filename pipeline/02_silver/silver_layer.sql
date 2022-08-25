-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Tables creation of Silver layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Table name: 
-- MAGIC 
-- MAGIC silver_categories
-- MAGIC 
-- MAGIC ### Description
-- MAGIC 
-- MAGIC Stores the clean information of the categories.
-- MAGIC Data validation is made before ingesting to this table.
-- MAGIC 
-- MAGIC ### Structure 
-- MAGIC 
-- MAGIC - __podcast_id (STRING)__: An unique identifier for a podcast.
-- MAGIC - __category (STRING)__: Category associated with the podcast.
-- MAGIC 
-- MAGIC ### Validations
-- MAGIC 
-- MAGIC - __podcast_id__ should not be null
-- MAGIC - __category__ should not be null

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_categories(
  CONSTRAINT valid_podcast_id EXPECT (podcast_id IS NOT NULL),
  CONSTRAINT valid_category EXPECT (category IS NOT NULL)
  )
COMMENT "The cleansed categories files."
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  podcast_id, 
  category 
FROM live.bronze_categories;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Table name: 
-- MAGIC 
-- MAGIC silver_reviews
-- MAGIC 
-- MAGIC ### Description
-- MAGIC 
-- MAGIC Stores clean information about the ratings and reviews made by the users about different podcasts.
-- MAGIC 
-- MAGIC ### Structure 
-- MAGIC 
-- MAGIC - __podcast_id (STRING)__: An unique identifier for a podcast.
-- MAGIC - __title_review (STRING)__: Title for the review. Works like a short summary of the long review.
-- MAGIC - __content (STRING)__: The long review about the podcast.
-- MAGIC - __rating (INT)__: The rating for the podcast given by the user. Between 0 and 5.
-- MAGIC - __author_id (STRING)__: An unique ID for the author of the review (user).
-- MAGIC - __created_at (TIMESTAMP)__: Timestamp when the review was created.
-- MAGIC 
-- MAGIC ### Validations
-- MAGIC 
-- MAGIC - __podcast_id__ should not be NULL
-- MAGIC - __creation_year_month__ should not be NULL. It could be null when created_at has a wrong format.
-- MAGIC - __rating__: Should be between 0 and 5.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_reviews(
  CONSTRAINT valid_podcast_id EXPECT (podcast_id IS NOT NULL),
  CONSTRAINT valid_creation_year_month  EXPECT (creation_year_month  IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_rating EXPECT (rating >=0 AND rating<=5) ON VIOLATION DROP ROW
  )
COMMENT "The cleansed reviews files."
TBLPROPERTIES ("quality" = "silver")
PARTITIONED BY (creation_year_month)
AS 
SELECT 
  podcast_id,
  title AS title_review,
  content,
  rating,
  author_id,
  CAST(created_at AS TIMESTAMP) AS created_at,
  YEAR(created_at) AS creation_year,
  MONTH(created_at) AS creation_month,
  DAY(created_at) AS creation_day,
  YEAR(created_at) * 100 + MONTH(created_at) as creation_year_month 
FROM live.bronze_reviews;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Table name: 
-- MAGIC 
-- MAGIC silver_podcasts
-- MAGIC 
-- MAGIC ### Description
-- MAGIC 
-- MAGIC Stores clean information about the podcast like url in iTunes and name of the podcast.
-- MAGIC 
-- MAGIC ### Structure 
-- MAGIC 
-- MAGIC - __podcast_id (STRING)__: An unique identifier for a podcast.
-- MAGIC - __itunes_id (STRING)__: ID of the podcast in iTunes.
-- MAGIC - __slug (STRING)__: Slug of the podcast.
-- MAGIC - __itunes_url (STRING)__: URL of the podcast in iTunes.
-- MAGIC - __title (STRING)__: The podcast name.
-- MAGIC 
-- MAGIC ### Validations
-- MAGIC 
-- MAGIC - __podcast_id__ should not be null

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_podcasts(
  CONSTRAINT valid_podcast_id EXPECT (podcast_id IS NOT NULL)
  )
COMMENT "The cleansed podcast files."
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  podcast_id, 
  itunes_id, 
  slug, 
  itunes_url, 
  title 
FROM live.bronze_podcasts;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Table name
-- MAGIC silver_reviews_podcasts
-- MAGIC 
-- MAGIC ### Description
-- MAGIC 
-- MAGIC Consolidates clean data from reviews, podcasts and categories in one table. Adds some new columns derived from created_at column.
-- MAGIC 
-- MAGIC ### Structure 
-- MAGIC 
-- MAGIC - __podcast_id (STRING)__: An unique identifier for a podcast.
-- MAGIC - __title (STRING)__: The podcast name.
-- MAGIC - __category (STRING)__: Category associated with the podcast.
-- MAGIC - __title_review (STRING)__: Title for the review. Works like a short summary of the long review.
-- MAGIC - __content (STRING)__: The long review about the podcast.
-- MAGIC - __rating (INT)__: The rating for the podcast given by the user. Between 0 and 5.
-- MAGIC - __author_id (STRING)__: An unique ID for the author of the review (user).
-- MAGIC - __created_at (TIMESTAMP)__: Timestamp when the review was created.
-- MAGIC - __created_year (INT)__: Year when the review was created.
-- MAGIC - __created_month (INT)__: Month when the review was created.
-- MAGIC - __creation_day (INT)__: Day when the review was created.
-- MAGIC - __created_year_month (INT)__: Year-Month when the review was created.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_reviews_podcasts
COMMENT "The cleansed full table."
TBLPROPERTIES ("quality" = "silver")
AS SELECT  
  a.podcast_id, 
  title, 
  category,
  title_review, 
  content, 
  rating, 
  author_id, 
  created_at, 
  creation_year, 
  creation_month, 
  creation_day, 
  creation_year_month
FROM live.silver_podcasts a 
INNER JOIN (
  SELECT 
    podcast_id, 
    COLLECT_SET(category) AS category 
  FROM live.silver_categories 
  GROUP BY podcast_id
  ) b
ON a.podcast_id = b.podcast_id
INNER JOIN live.silver_reviews c
ON a.podcast_id = c.podcast_id;