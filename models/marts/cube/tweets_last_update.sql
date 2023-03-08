{{ config(tags=["cube"]) }}

WITH tweets_last_updated as (
  SELECT task_id as task_id_1, max(processed_at) as last_update
  FROM {{ ref('tweets_insights') }}
  GROUP BY 1
),
tweets_unique as (
SELECT
    distinct *
FROM
    {{ ref('tweets_insights') }} ti
INNER JOIN
  tweets_last_updated tlu
ON
  ti.task_id=tlu.task_id_1 and ti.processed_at=tlu.last_update
)
SELECT
    campaign_id,
    influencer_id,
    first_name,
    last_name,
    task_id,
    submission_status,
    first_verification_status,
    second_verification_status,
    third_verification_status,
    tweet_id,
    tweet_text,
    username,
    submission_link,
    channel,
    created_at,
    error,
    followers_count,
    hashtags,
    likes,
    location,
    possibly_sensitive,
    profile_location,
    quote_count,
    reply_count,
    retweet_count,
    impressions,
    source,
    stage,
    status,
    processed_at
FROM
    tweets_unique