SELECT
    id,
    campaign_id,
    channel,
    description
FROM {{ source('staging', 'campaign_tasks') }}