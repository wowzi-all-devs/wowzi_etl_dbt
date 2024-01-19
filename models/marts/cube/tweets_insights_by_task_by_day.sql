{{ config(
    materialized = 'view',
    tags=["cube"]
) }}
WITH unique_tasks_by_day as (
  SELECT 
    task_id as task_id_1, DATE(processed_at), max(processed_at) as last_update
  FROM
    {{ ref('tweets_insights') }}
  GROUP BY 1, 2
),
tweets_unique as (
  SELECT
    distinct *
  FROM
    {{ ref('tweets_insights') }} ti
  INNER JOIN
    unique_tasks_by_day ut
  ON
    ti.task_id=ut.task_id_1 and ti.processed_at=ut.last_update
)
SELECT 
  DATE(processed_at) as processed_at,
  task_id,
  campaign_id,
  influencer_id,
  likes,
  reply_count as comments,
  retweet_count as retweets,
  impressions
FROM
  tweets_unique
ORDER BY processed_at
  
