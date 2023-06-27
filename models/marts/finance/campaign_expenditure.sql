SELECT * FROM(
SELECT
    campaign_id,
    campaign_name,
    campaign_start_date,
    campaign_end_date,
    campaign_bonus_amount,
    campaign_budget,
    budget_spent,
    agency_commission,
    influencers_invited_count,
    influencers_accepted_count,
    merchant_id,
    merchant_name,
    company_name,
    currency,
    canceled,
    row_number() over(partition by campaign_id order by campaign_id) as row_number

FROM {{ source('twitter_api', 'campaign_expenditure') }}
) where row_number=1