{{ config(tags=["finance"]) }}

WITH dims_payments AS (
SELECT      
    {{ dbt_utils.surrogate_key(['inf_transfers.id', 'inf_transfers.influencer_id', 'inf_tasks.campaign_id', 'inf_transfers.task_id']) }} as primary_key,
    inf_transfers.influencer_id as influencer_id,
    inf.mobile_number,
    inf.gender,
    inf.age_range,
    inf.influencer_type_TWITTER,
    inf.influencer_type_FACEBOOK,
    inf.influencer_type_LINKEDIN,
    inf.influencer_type_TIKTOK,
    inf_tasks.campaign_id,
    companies.name as company_name,
    adv.advertiser_id,
    adv.first_name as advertiser_fname,
    adv.last_name as advertiser_lname,
    campaigns.name as campaign_name,
    inf_transfers.task_id,
    (CASE WHEN lower(inf_tasks.submission_link) LIKE '%twitter%' THEN 'Twitter'
    WHEN lower(inf_tasks.submission_link) LIKE '%t.co%' THEN 'Twitter'
    WHEN lower(inf_tasks.submission_link) LIKE '%facebook%' THEN 'Facebook'
    WHEN lower(inf_tasks.submission_link) LIKE '%linkedin%' THEN 'LinkedIn'
    WHEN lower(inf_tasks.submission_link) LIKE '%tiktok%' THEN 'TikTok'
    WHEN lower(inf_tasks.submission_link) LIKE '%instagram%' THEN 'Instagram'
    WHEN lower(inf_tasks.submission_link) LIKE '%youtube%' THEN 'YouTube'
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
   LEFT JOIN {{ ref('bank_details') }} bd USING (influencer_id)
   LEFT JOIN {{ ref('influencer_tasks') }} inf_tasks ON inf_tasks.id = inf_transfers.task_id
   LEFT JOIN {{ ref('campaigns') }} campaigns ON campaigns.id=inf_tasks.campaign_id
   LEFT join {{ ref('dims_advertisers') }} adv ON adv.advertiser_id=campaigns.merchant_id
   LEFT join {{ ref('companies') }} companies ON companies.id = campaigns.company_id
   LEFT JOIN {{ ref('currency_rates') }} rates ON cast(left(inf_transfers.date_created, 10) as date) = cast(rates.date as date)
    AND upper(inf_transfers.currency) = upper(rates.currency)
)

SELECT
    primary_key,
    influencer_id,
    mobile_number,
    gender,
    age_range,
    advertiser_id,
    campaign_id,
    task_id,
    social_media_channel,
    (case when social_media_channel = 'Twitter'
    then influencer_type_TWITTER
    when social_media_channel = 'Facebook'
    then influencer_type_FACEBOOK
    when social_media_channel = 'Linkedin'
    then influencer_type_LINKEDIN
    when social_media_channel = 'Tiktok'
    then influencer_type_TIKTOK
    end) as task_influencer_type,
    campaign_name,
    payment_date,
    country,
    amount,
    amount_usd,
    currency,
    payment_status,
    bank_name,
    company_name 
FROM dims_payments
