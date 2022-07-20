WITH first_task_ids AS (
    SELECT influencer_id,
        campaign_creation_date AS first_campaign_date,
        campaign_task_id AS first_campaign_task_id,
        FROM (
            SELECT influencer_id,
                DATE(TIMESTAMP(creation_time)) AS campaign_creation_date,
                campaign_task_id,
                ROW_NUMBER() OVER(
                    PARTITION BY influencer_id
                    ORDER BY TIMESTAMP(creation_time) ASC
                ) AS row_number,
                -- FROM `bi-staging-1-309112.influencer_tasks.influencer_tasks_{}`
                FROM {{ source('staging', 'influencer_tasks') }}
            ORDER BY influencer_id DESC
        )
    WHERE row_number = 1
)
SELECT 
    influencer_id,
    first_campaign_task_id,
    True as first_job_complete_and_paid
FROM first_task_ids
WHERE first_campaign_task_id IN (
    SELECT task_id,
    -- FROM `bi-staging-1-309112.influencer_transfers.influencer_transfers_{}`
    FROM {{ source('staging', 'influencer_transfers') }}
    WHERE DATE_DIFF(DATE(date_created),DATE(CURRENT_TIMESTAMP()),DAY) <= 28
)