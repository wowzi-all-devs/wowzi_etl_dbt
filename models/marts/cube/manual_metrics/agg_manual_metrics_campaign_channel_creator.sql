{{ config(tags=["cube","manual_metrics"]) }}

select
    campaign_id,
    task_id,
    influencer_id,
    influencer_name,
    channel,
    submission_link,
    likes,
    comments,
    views,
    shares,
    (views + shares + likes + comments) as engagement,
    followers_count,
    influencer_type,
    updated_at

from {{ ref('stg__manual_metrics') }}