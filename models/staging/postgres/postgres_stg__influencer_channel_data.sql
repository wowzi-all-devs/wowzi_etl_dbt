SELECT 
    id,
    channel,
    influencer_id,
    cast(followers_count as INT) as followers_count,
    username,
    initcap(status) as status,
    is_system_linked,
    status_backofficer_id,
    status_comment,
    status_update_time
FROM {{ source('staging', 'influencer_channel_data') }}