{{ config(tags=["manual_metrics"]) }}

select
    campaign_id,
    channel,
    influencer_id,
    task_id,
    submission_link,
    views as total_views,
    shares as total_shares,
    likes as total_likes,
    comments as total_comments,
    (views + shares + likes + comments) as total_engagement,
    followers_count as follower_count,
    updated_at

from {{ ref('stg__manual_metrics') }}