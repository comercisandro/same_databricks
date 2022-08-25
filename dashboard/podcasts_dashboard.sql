-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Distribution of podcasts among categories

-- COMMAND ----------

SELECT
  CASE WHEN category_rank > 9 THEN 'other' ELSE category END AS category,
  ROUND(sum(percentage), 2) AS percentage_of_podcasts,
  sum(count_podcasts) AS number_of_podcasts,
  COUNT(DISTINCT category) AS number_of_categories
FROM (
  SELECT 
    category, 
    count_podcasts,
    count_podcasts/total*100 AS percentage,
    RANK() OVER (ORDER BY count_podcasts/total*100 DESC) AS category_rank
  FROM same_db.gold_podcast_stats
  JOIN (SELECT sum(count_podcasts) AS total FROM same_db.gold_podcast_stats)
 )
 GROUP BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Which is the most popular podcast?
-- MAGIC 
-- MAGIC <span style="color: red">Que sería más popular? Hay podcasts que pueden ser populares pero la gente no hacen reviews.</span>.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_podcasts_by_category
COMMENT "The aggregated table in gold layer."
TBLPROPERTIES ("quality" = "gold")
AS
WITH q1 AS(
SELECT
  category, 
  COUNT(distinct podcast_id) as count_podcasts,
  total
FROM live.silver_reviews_podcasts
JOIN (SELECT COUNT(DISTINCT podcast_id) AS total FROM live.silver_reviews_podcasts)
GROUP BY category, total)
SELECT
  category,
  count_podcasts / total * 100 AS percentage_of_podcasts,
  count_podcasts
FROM q1;
  

-- COMMAND ----------

'''Aggregations from silver to gold: 

-Columns: podcast_id, title, title_review, content, rating, created_at, year_month, author_id, category

number of reviews with each rating  
which is the most popular title?  _OK_
how many reviews does the podcast with the min number of reviews have? And with the max? _OK_
year_month when most of the reviews were written _OK_
what is the author_id with the highest number of titles? _OK_
what is the largest category (whith the max number of titles)? _OK_
which is the best/worst rated category? _OK_
average rating for each category _OK_
'''
