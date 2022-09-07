{{ config(tags=["influencer_success"]) }}

WITH metrics_expenditure_joined as (
SELECT
    td.influencer_id,
    td.task_id,
    td.campaign_id,
    td.tweet_id,
    td.channel,
    td.followers_count,
    td.quote_count,
    td.reply_count,
    td.likes,
    td.retweet_count,
    td.username,
    td.processed_at,
    td.created_at as post_created_at,
    td.submission_link,
    ce.campaign_name,
    ce.budget_spent,
    ce.merchant_id,
    ce.merchant_name,
    ce.company_name,
    ROW_NUMBER() OVER(PARTITION BY td.campaign_id, td.task_id, td.influencer_id
                        ORDER BY td.processed_at DESC) AS row_number,
FROM 
    {{ ref('twitter_campaign_data') }} td
LEFT JOIN 
    {{ ref('campaign_expenditure') }} ce
ON td.campaign_id = CAST(ce.campaign_id AS int)
WHERE 
    td.username IS NOT NULL 
order by
    processed_at desc
)
SELECT 
    {{ dbt_utils.surrogate_key(['influencer_id', 'campaign_id', 'task_id']) }} as primary_key,
    *
FROM
    metrics_expenditure_joined
WHERE 
    row_number=1
ORDER BY 
    processed_at desc