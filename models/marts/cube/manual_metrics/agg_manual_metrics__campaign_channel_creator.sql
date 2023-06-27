{{ config(tags=["cube"]) }}

select
    campaign_id,
    channel,
    influencer_id,
    sum(views) as total_views,
    sum(shares) as total_shares,
    sum(likes) as total_likes,
    sum(comments) as total_comments,
    (sum(views)+sum(shares)+sum(likes)+sum(comments)) as total_engagement,
    sum(followers_count) as potential_reach,
    max(updated_at) as updated_at

from {{ ref('stg__manual_metrics') }}  group by 1, 2, 3