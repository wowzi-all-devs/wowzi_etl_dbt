select influencer_id,
    campaign_creation_date as last_campaign_date
from (
        SELECT influencer_id,
            date(timestamp(creation_time)) as campaign_creation_date,
            ROW_NUMBER() OVER(
                PARTITION BY influencer_id
                ORDER BY timestamp(creation_time) desc
            ) as row_number,
            FROM {{ source('staging', 'influencer_tasks') }}
        order by influencer_id desc
    )
where row_number = 1