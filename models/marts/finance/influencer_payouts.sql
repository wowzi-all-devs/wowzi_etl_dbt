{{ config(tags=["finance"]) }}

WITH dims_payments AS (
SELECT      
    inf_transfers.id as transfer_id,
    inf_transfers.influencer_id as influencer_id,
    inf.mobile_number,
    inf.gender,
    inf.age_range,
    inf.influencer_type_TWITTER,
    inf.influencer_type_FACEBOOK,
    inf.influencer_type_LINKEDIN,
    inf.influencer_type_TIKTOK,
    inf.influencer_type_INSTAGRAM,
    inf_tasks.campaign_id,
    companies.name as company_name,
    adv.advertiser_id,
    adv.first_name as advertiser_fname,
    adv.last_name as advertiser_lname,
    campaigns.name as campaign_name,
    inf_transfers.task_id,
    (CASE WHEN lower(inf_tasks.submission_link) LIKE '%twitter%' THEN 'TWITTER'
    WHEN lower(inf_tasks.submission_link) LIKE '%t.co%' THEN 'TWITTER'
    WHEN lower(inf_tasks.submission_link) LIKE '%facebook%' THEN 'FACEBOOK'
    WHEN lower(inf_tasks.submission_link) LIKE '%linkedin%' THEN 'LINKEDIN'
    WHEN lower(inf_tasks.submission_link) LIKE '%tiktok%' THEN 'TIKTOK'
    WHEN lower(inf_tasks.submission_link) LIKE '%instagram%' THEN 'INSTAGRAM'
    WHEN lower(inf_tasks.submission_link) LIKE '%youtube%' THEN 'YOUTUBE'
    WHEN lower(inf_tasks.submission_link) IS NULL THEN 'NOT-SUBMITTED'
    end) as social_media_channel,
    inf_transfers.amount,
    inf_transfers.amount/rates.currency_rate as amount_usd,
    inf_transfers.status as payment_status,
    inf_transfers.date_created as payment_date,
    inf_transfers.currency as currency,
    inf.country,
    bd.bank_name as bank_name,
    bd.bank_account_number,
    inf_transfers.narration,
    inf_transfers.reference
FROM {{ ref('influencer_transfers') }} inf_transfers
   LEFT JOIN {{ ref('influencer_facts') }} inf USING (influencer_id)
   LEFT JOIN {{ ref('stg_bank_details') }} bd USING (influencer_id)
   LEFT JOIN {{ ref('influencer_tasks') }} inf_tasks ON inf_tasks.id = inf_transfers.task_id
   LEFT JOIN {{ ref('campaigns') }} campaigns ON campaigns.id=inf_tasks.campaign_id
   LEFT join {{ ref('dims_advertisers') }} adv on adv.advertiser_id=campaigns.merchant_id
   LEFT join {{ ref('companies') }} companies on companies.id = campaigns.company_id
   LEFT JOIN {{ ref('currency_rates') }} rates ON cast(left(inf_transfers.date_created, 10) as date) = cast(rates.date as date)
    AND upper(inf_transfers.currency) = upper(rates.currency)
)
SELECT
    transfer_id,
    influencer_id,
    mobile_number,
    gender,
    age_range,
    advertiser_id,
    campaign_id,
    task_id,
    social_media_channel,
    (CASE WHEN social_media_channel = 'TWITTER'
    THEN influencer_type_TWITTER
    WHEN social_media_channel = 'FACEBOOK'
    THEN influencer_type_FACEBOOK
    WHEN social_media_channel = 'LINKEDIN'
    THEN influencer_type_LINKEDIN
    WHEN social_media_channel = 'TIKTOK'
    THEN influencer_type_TIKTOK
    WHEN social_media_channel = 'INSTAGRAM'
    THEN influencer_type_INSTAGRAM
    END) as task_influencer_type,
    campaign_name,
    payment_date,
    country,
    amount,
    amount_usd,
    currency,
    payment_status,
    bank_name,
    company_name,
    reference,
    narration
FROM 
    dims_payments