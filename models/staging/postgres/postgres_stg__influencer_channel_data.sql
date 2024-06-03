SELECT 
    id,
    channel,
    influencer_id,
    cast(followers_count as INT) as followers_count ,
    username,
    status,
    status_backofficer_id,
    status_comment,
    status_update_time
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_channel_data` 