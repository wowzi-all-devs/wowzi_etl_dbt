SELECT
    influencer_id,
    influencer_group_id,
    creation_time
FROM {{ source('staging', 'influencer_group_influencers') }}