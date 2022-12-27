SELECT
    campaign_name,
    campaign_id,
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
    canceled
FROM {{ source('twitter_api', 'campaign_expenditure') }}