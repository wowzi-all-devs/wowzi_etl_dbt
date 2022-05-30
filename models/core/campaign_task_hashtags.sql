SELECT
    id,
    value,
    campaign_id,
    campaign_task_id
FROM {{ source('core', 'campaign_task_hashtags') }}