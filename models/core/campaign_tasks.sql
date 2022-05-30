SELECT
    id,
    campaign_id,
    channel,
    description
FROM {{ source('core', 'campaign_tasks') }}