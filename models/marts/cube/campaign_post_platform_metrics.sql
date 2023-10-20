{{ config(tags=["cube"]) }}

SELECT
  tweets.task_id,
  upper(channel) as channel,
  campaign_id,
  influencer_id,
  submission_link,
  likes,
  NULL AS views,
  retweet_count as shares,
  reply_count as comments,
  quote_count as quotes,
  impressions,
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
  upper(channel) as channel,
  campaign_id,
  influencer_id,
  submission_link,
  likes,
  views,
  shares,
  comments,
  NULL as quotes,
  NULL as impressions,
  TIMESTAMP(updated_at) AS updated_at
FROM
  {{ ref('stg__manual_metrics') }}