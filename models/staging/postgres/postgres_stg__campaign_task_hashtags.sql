SELECT
    id,
    value,
    campaign_id,
    campaign_task_id
FROM {{ source('staging', 'campaign_task_hashtags') }}