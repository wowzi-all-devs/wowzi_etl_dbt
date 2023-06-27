{{ config(tags=["manual_metrics"]) }}

select
    campaign_id,
    channel,
    count(distinct influencer_id) as total_influencers,
    sum(views) as total_views,
    sum(shares) as total_shares,
    sum(likes) as total_likes,
    sum(comments) as total_comments,
    (sum(views)+sum(shares)+sum(likes)+sum(comments)) as total_engagement,
    sum(followers_count) as total_follower_count,
    max(updated_at) as updated_at

from {{ ref('stg__manual_metrics') }}  group by 1, 2