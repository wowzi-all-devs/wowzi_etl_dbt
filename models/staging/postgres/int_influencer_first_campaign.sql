select influencer_id,
    campaign_creation_date as first_campaign_date
from (
        SELECT influencer_id,
            date(timestamp(creation_time)) as campaign_creation_date,
            ROW_NUMBER() OVER(
                PARTITION BY influencer_id
                ORDER BY timestamp(creation_time) asc
            ) as row_number,
            FROM {{ ref('postgres_stg__influencer_tasks') }}
        order by influencer_id desc
    )
where row_number = 1

-- IN the SELECT inside FROM perhaps is possible get only tasks of the last day.