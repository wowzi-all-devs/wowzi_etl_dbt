{{ config(tags=["finance"]) }}

WITH payments AS (
SELECT      
    inf_transfers.id as transfer_id,
    inf_transfers.influencer_id,
    inf.first_name || ' ' || inf.last_name as influencer_name,
    inf.mobile_number,
    inf.gender,
    inf.influencer_type_TWITTER,
    inf.influencer_type_FACEBOOK,
    inf.influencer_type_LINKEDIN,
    inf.influencer_type_TIKTOK,
    inf.influencer_type_INSTAGRAM,
    inf_tasks.campaign_id,
    companies.company_name,
    adv.advertiser_id,
    adv.first_name as advertiser_fname,
    adv.last_name as advertiser_lname,
    campaigns.campaign_name,
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
    date(inf_transfers.date_created) as payment_date,
    inf_transfers.currency as currency,
    inf.country,
    bd.bank_name as bank_name,
    bd.bank_account_number,
    inf_transfers.narration,
    inf_transfers.reference
FROM {{ ref('postgres_stg__influencer_transfers') }} inf_transfers
   LEFT JOIN {{ ref('influencer_facts') }} inf USING (influencer_id)
   LEFT JOIN {{ ref('int_bank_details') }} bd USING (influencer_id)
   INNER JOIN {{ ref('postgres_stg__influencer_tasks') }} inf_tasks ON inf_tasks.task_id = inf_transfers.task_id
   LEFT JOIN {{ ref('postgres_stg__campaigns') }} campaigns ON campaigns.campaign_id=inf_tasks.campaign_id
   LEFT join {{ ref('postgres_stg__merchants') }} adv on adv.advertiser_id=campaigns.merchant_id
   LEFT join {{ ref('postgres_stg__companies') }} companies on companies.company_id = campaigns.company_id
   LEFT JOIN {{ ref('int_currency_rates') }} rates ON cast(inf_transfers.date_created as date) = cast(rates.date as date)
   AND upper(inf_transfers.currency) = upper(rates.currency)
),

dims_payments as (
SELECT
    transfer_id,
    influencer_id,
    influencer_name,
    mobile_number,
    gender,
    advertiser_id,
    campaign_id,
    task_id,
    campaign_name,
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
    payments)
    
select * from dims_payments