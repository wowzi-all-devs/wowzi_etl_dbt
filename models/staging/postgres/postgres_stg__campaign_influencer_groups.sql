SELECT
    campaign_id,
    influencer_group_id,
    creation_time
FROM {{ source('staging', 'campaign_influencer_groups') }}