{{ config(tags=["cube"]) }}

SELECT
    twitter_data.campaign_id,
    twitter_data.influencer_id,
    influencers.first_name,
    influencers.last_name,
    concat(influencers.first_name, " ", influencers.last_name) as influencer_name,
    twitter_data.task_id,
    tasks.submission_status,
    tasks.first_verification_status,
    tasks.second_verification_status,
    tasks.third_verification_status,
    tweet_id,
    tweet_text,
    twitter_data.username,
    twitter_data.submission_link,
    twitter_data.channel,
    twitter_data.created_at,
    twitter_data.error,
    twitter_data.followers_count,
    twitter_data.hashtags,
    twitter_data.likes,
    twitter_data.location,
    twitter_data.possibly_sensitive,
    twitter_data.profile_location,
    twitter_data.quote_count,
    twitter_data.reply_count,
    twitter_data.retweet_count,
    if(twitter_data.impressions IS NOT NULL, twitter_data.impressions, 0) as impressions,
    twitter_data.source,
    twitter_data.stage,
    twitter_data.status,
    twitter_data.processed_at
FROM
    {{ ref('twitter_campaign_data') }} twitter_data
LEFT JOIN {{ ref('postgres_stg__influencers') }} influencers on twitter_data.influencer_id = influencers.influencer_id
LEFT JOIN {{ ref('postgres_stg__influencer_tasks') }} tasks on twitter_data.task_id = tasks.task_id
WHERE
    twitter_data.error='' and twitter_data.tweet_text is not null and twitter_data.username is not null