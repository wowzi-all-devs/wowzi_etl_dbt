{{ config(
    materialized = 'materialized_view',
    on_configuration_change = 'apply',
    enable_refresh = True,
    refresh_interval_minutes = 30,
    )
}}
SELECT
    task_id,
    influencer_id,
    campaign_id,
    tweet_id,
    channel,
    tweet_text,
    error,
    followers_count,
    hashtags,
    likes,
    location,
    possibly_sensitive,
    created_at,
    processed_at,
    profile_location,
    quote_count,
    reply_count,
    retweet_count,
    impressions,
    source,
    stage,
    status,
    submission_link,
    username
FROM {{ source('twitter_api', 'twitter_campaign_data_redux') }}