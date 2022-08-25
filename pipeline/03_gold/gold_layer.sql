-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Tables creation of Gold layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Table name: 
-- MAGIC 
-- MAGIC gold_podcasts_by_category
-- MAGIC 
-- MAGIC ### Description
-- MAGIC 
-- MAGIC Stores aggregated information: Distribution of numbers of podcasts by categories.
-- MAGIC 
-- MAGIC ### Structure 
-- MAGIC 
-- MAGIC - __category (STRING)__: A unique category.
-- MAGIC - __count_podcast_id (INT)__: Number of podcasts with reviews for in a given category.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_podcasts_by_category
COMMENT "Count of unique podcasts by categories."
TBLPROPERTIES ("quality" = "gold")
AS
WITH q1 AS (
  SELECT 
    podcast_id, 
    EXPLODE(category) AS category 
  FROM live.silver_reviews_podcasts
)
SELECT 
  category, 
  COUNT(DISTINCT podcast_id) AS count_podcast_id 
FROM q1
GROUP BY category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Table name: 
-- MAGIC 
-- MAGIC gold_podcasts_stats
-- MAGIC 
-- MAGIC ### Description
-- MAGIC 
-- MAGIC Stores aggregated information: Stats about rating of every podcast.
-- MAGIC 
-- MAGIC ### Structure 
-- MAGIC 
-- MAGIC - __title (STRING)__: Name of the podcast.
-- MAGIC - __category (STRING)__: Category associated to the podcast.
-- MAGIC - __number_of_reviews (INT)__: Number of reviews for the podcast.
-- MAGIC - __mean_rating (FLOAT)__: Average rating for the title.
-- MAGIC - __median_rating (FLOAT)__: Median rating for the title.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_podcasts_stats
COMMENT "Podcast rating stats by podcast."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  title,
  category,
  COUNT(*) AS number_of_reviews,
  ROUND(AVG(rating), 2) AS mean_rating,
  percentile_approx(rating, 0.5) AS median_rating
FROM live.silver_reviews_podcasts 
GROUP BY title, category

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Table name: 
-- MAGIC 
-- MAGIC gold_monthly_reviews
-- MAGIC 
-- MAGIC ### Description
-- MAGIC 
-- MAGIC Stores aggregated information: Reviews by category and year-month.
-- MAGIC 
-- MAGIC ### Structure 
-- MAGIC 
-- MAGIC - __creation_year_month (INT)__: Year-Month when the review was created.
-- MAGIC - __category (STRING)__: Category associated to the podcast.
-- MAGIC - __number_of_reviews (INT)__: Number of reviews for the podcast.

-- COMMAND ----------

--year_month when most of the reviews were written
CREATE OR REFRESH LIVE TABLE gold_monthly_reviews
COMMENT "Podcast rating stats by podcast."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
  creation_year_month, 
  category, 
  COUNT(*) AS number_of_reviews
FROM live.silver_reviews_podcasts
GROUP BY creation_year_month, category;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC -- NO FUNCIONA, VER
-- MAGIC WITH q1 AS(
-- MAGIC SELECT
-- MAGIC   author_id,
-- MAGIC   rating,
-- MAGIC   COUNT(DISTINCT podcast_id) AS number_of_podcasts
-- MAGIC FROM same_db.silver_reviews_podcasts
-- MAGIC --WHERE author_id = "4946DE8A43AE955"
-- MAGIC GROUP BY author_id, rating)
-- MAGIC SELECT author_id FROM q1
-- MAGIC PIVOT (SUM(number_of_podcasts)AS number_of_podcasts
-- MAGIC   FOR rating 
-- MAGIC   IN (0 AS rating_0, 1 AS rating_1, 2 AS rating_2, 3 AS rating_3, 4 AS rating_4, 5 AS rating_5));