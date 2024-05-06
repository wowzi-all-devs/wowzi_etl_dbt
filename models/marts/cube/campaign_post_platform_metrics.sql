{{ config(
    materialized = 'view',
    tags=["cube"]
) }}
SELECT
  tweets.task_id,
  upper(channel) as channel,
  campaign_id,
  influencer_id,
  influencer_name,
  followers_count as influencer_followers,
  submission_link,
  likes,
  NULL AS views,
  retweet_count as shares,
  reply_count as comments,
  quote_count as quotes,
  impressions,
  reach_percentage,
  TIMESTAMP_TRUNC(processed_at, SECOND) AS updated_at
FROM
  {{ref('tweets_insights')}} tweets
INNER JOIN (
  SELECT
    task_id,
    MAX(processed_at) AS mx_date
  FROM
     {{ref('tweets_insights')}}
  GROUP BY
    task_id ) ms
ON
  ms.task_id=tweets.task_id and
  ms.mx_date=processed_at
UNION DISTINCT
SELECT
  task_id,
  ifnull(upper(channel),"") as channel,
  campaign_id,
  influencer_id,
  influencer_name,
  ifnull(followers_count,0)as influencer_followers,
  submission_link,
  likes,
  views,
  shares,
  comments,
  NULL as quotes,
  NULL as impressions,
  NULL as reach_percentage,
  TIMESTAMP(updated_at) AS updated_at
FROM
  {{ ref('stg__manual_metrics') }}