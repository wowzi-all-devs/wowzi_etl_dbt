SELECT
  tweets.task_id,
  upper(channel) as channel,
  campaign_id,
  influencer_id,
  submission_link,
  likes,
  NULL AS views,
  NULL AS comments,
  NULL AS shares,
  retweet_count,
  quote_count,
  reply_count,
  impressions,
  DATETIME(processed_at) AS processed_at
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
  comments,
  shares,
  NULL,
  NULL,
  NULL,
  NULL,
  updated_at AS processed_at
FROM
  {{ ref('stg__manual_metrics') }}