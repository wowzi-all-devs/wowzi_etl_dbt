SELECT
    id,
    channel,
    influencer_id,
    api_key,
    api_id,
    followers_count,
    username,
    status,
    status_backofficer_id,
    status_comment,
    status_update_time
FROM {{ source('core', 'influencer_channel_data') }}