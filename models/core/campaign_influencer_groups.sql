SELECT
    campaign_id,
    influencer_group_id,
    creation_time
FROM {{ source('core', 'campaign_influencer_groups') }}