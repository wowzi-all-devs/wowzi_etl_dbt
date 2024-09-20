{{ config(
    materialized='incremental',
    partition_by={
      "field": "processed_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["campaign_id", "influencer_id"],
    on_schema_change = "sync_all_columns",
    incremental_strategy = "merge",
    incremental_predicates = [
        "DBT_INTERNAL_DEST.processed_at > timestamp_add(current_timestamp(), interval -1 day)"],
)}}
SELECT
    task_id,
    influencer_id,
    campaign_id,
    tweet_id,
    channel,
    error,
    followers_count,
    hashtags,
    likes,
    location,
    possibly_sensitive,
    created_at,
    processed_at,
    profile_location,
    ifnull(quote_count,0) as quote_count,
    ifnull(reply_count,0) as reply_count,
    ifnull(retweet_count,0) as retweet_count,
    ifnull(impressions,0) as impressions,
    submission_link,
    username
FROM {{ source('twitter_api', 'raw_twitter__campaign_data') }}
 where  (error='' or error='None') and username is not null
{% if is_incremental() %}
    and processed_at >=(select max(processed_at) from {{ this }})
{% endif %}