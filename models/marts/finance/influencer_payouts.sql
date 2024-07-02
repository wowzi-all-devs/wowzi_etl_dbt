{{ config(tags=["finance"]) }}

WITH payments AS 
(
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
    inf_transfers.status as payment_status,
    case when inf_transfers.date_created is null then date(inf_transfers.updated_at)
    else date(inf_transfers.date_created) end as payment_date,
    inf_transfers.updated_at,
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
),

payments_with_usd AS
(
SELECT 
    p.transfer_id,
    p.influencer_id,
    p.influencer_name,
    p.mobile_number,
    p.gender,
    p.influencer_type_TWITTER,
    p.influencer_type_FACEBOOK,
    p.influencer_type_LINKEDIN,
    p.influencer_type_TIKTOK,
    p.influencer_type_INSTAGRAM,
    p.campaign_id,
    p.company_name,
    p.advertiser_id,
    p.advertiser_fname,
    p.advertiser_lname,
    p.campaign_name,
    p.task_id,
    p.social_media_channel,
    p.amount,
    case 
        when p.payment_date <= '2020-06-01'
        then p.amount/106.78
        when rates.currency_rate is not NULL
        then p.amount/rates.currency_rate 
        when rates.currency_rate is NULL
        then p.amount/rates2.currency_rate
    end amount_usd,
    p.payment_status,
    p.payment_date,
    p.updated_at,
    p.currency,
    p.country,
    p.bank_name,
    p.bank_account_number,
    p.narration,
    p.reference
FROM payments p 
LEFT JOIN {{ ref('int_currency_rates') }} rates ON date(p.payment_date) = date(rates.date)
AND upper(p.currency) = upper(rates.currency)
LEFT JOIN {{ ref('int_currency_rates') }} rates2 ON date(p.payment_date) = date_sub(date(rates2.date), INTERVAL 2 DAY)
AND upper(p.currency) = upper(rates2.currency)
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
    updated_at,
    country,
    amount,
    amount_usd,
    currency,
    case when date(payment_date) <= date('2023-11-30') and lower(payment_status) = 'failed'
    then 'Manual'
    else payment_status
    end payment_status,
    bank_name,
    company_name,
    reference,
    narration
FROM 
    payments_with_usd)
    
select 
    *
from dims_payments