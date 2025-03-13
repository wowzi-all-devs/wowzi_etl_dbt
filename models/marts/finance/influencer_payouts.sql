{{ config(tags=["finance"]) }}

WITH payments AS 
(
SELECT      
    inf_transfers.id transfer_id,
    inf_transfers.influencer_id,
    inf.first_name || ' ' || inf.last_name influencer_name,
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
    adv.first_name advertiser_fname,
    adv.last_name advertiser_lname,
    campaigns.campaign_name,
    inf_transfers.task_id,
    CASE 
        WHEN lower(inf_tasks.submission_link) LIKE '%twitter%' THEN 'TWITTER'
        WHEN lower(inf_tasks.submission_link) LIKE '%t.co%' THEN 'TWITTER'
        WHEN lower(inf_tasks.submission_link) LIKE '%facebook%' THEN 'FACEBOOK'
        WHEN lower(inf_tasks.submission_link) LIKE '%linkedin%' THEN 'LINKEDIN'
        WHEN lower(inf_tasks.submission_link) LIKE '%tiktok%' THEN 'TIKTOK'
        WHEN lower(inf_tasks.submission_link) LIKE '%instagram%' THEN 'INSTAGRAM'
        WHEN lower(inf_tasks.submission_link) LIKE '%youtube%' THEN 'YOUTUBE'
        WHEN lower(inf_tasks.submission_link) IS NULL THEN 'NOT-SUBMITTED'
    END social_media_channel,
    inf_transfers.amount,
    inf_transfers.status payment_status,
    inf_transfers.payment_eligible_at,
    FORMAT_DATE('%a',inf_transfers.payment_eligible_at) AS payment_eligible_at_date_weekday_name,
    CASE 
        WHEN inf_transfers.date_created IS NULL THEN DATE(inf_transfers.updated_at)
        ELSE DATE(inf_transfers.date_created) 
    END payment_date,
    inf_transfers.updated_at,
    inf_transfers.currency currency,
    inf_transfers.exchange_rate,
    inf.country,
    bd.bank_name bank_name,
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

latest_rates AS
(
SELECT 
  a.date,
  a.currency,
  b.currency_rate
FROM
(
SELECT 
  max(date) date,
  currency
FROM {{ ref('int_currency_rates') }} 
    WHERE base = 'USD'
    GROUP BY currency
) a
LEFT JOIN {{ ref('int_currency_rates') }} b 
ON a.date = b.date AND a.currency = b.currency
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
    CASE 
        WHEN p.exchange_rate IS NOT NULL 
        THEN p.amount*p.exchange_rate
        WHEN p.payment_date <= '2020-06-01'
        THEN p.amount/106.78
        WHEN rates.currency_rate IS NOT NULL
        THEN p.amount/rates.currency_rate 
        WHEN rates.currency_rate IS NULL AND rates2.currency_rate IS NOT NULL
        THEN p.amount/rates2.currency_rate
        ELSE p.amount/rates3.currency_rate
    END amount_usd,
    p.payment_status,
    p.payment_eligible_at,
    p.payment_eligible_at_date_weekday_name,
    p.payment_date,
    p.updated_at,
    p.currency,
    p.exchange_rate,
    p.country,
    p.bank_name,
    p.bank_account_number,
    p.narration,
    p.reference
FROM payments p 
    LEFT JOIN {{ ref('int_currency_rates') }} rates ON DATE(p.payment_date) = DATE(rates.date)
    AND UPPER(p.currency) = UPPER(rates.currency)
    LEFT JOIN {{ ref('int_currency_rates') }} rates2 ON DATE(p.payment_date) = DATE_SUB(DATE(rates2.date), INTERVAL 3 DAY)
    AND UPPER(p.currency) = UPPER(rates2.currency)
    LEFT JOIN latest_rates rates3 ON UPPER(p.currency) = UPPER(rates3.currency)
),

dims_payments AS (
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
    CASE 
        WHEN social_media_channel = 'TWITTER'
        THEN influencer_type_TWITTER
        WHEN social_media_channel = 'FACEBOOK'
        THEN influencer_type_FACEBOOK
        WHEN social_media_channel = 'LINKEDIN'
        THEN influencer_type_LINKEDIN
        WHEN social_media_channel = 'TIKTOK'
        THEN influencer_type_TIKTOK
        WHEN social_media_channel = 'INSTAGRAM'
        THEN influencer_type_INSTAGRAM
    END task_influencer_type,
    payment_eligible_at,
    payment_eligible_at_date_weekday_name,
    payment_date,
    updated_at,
    country,
    amount,
    amount_usd,
    currency,
    exchange_rate,
    CASE 
        WHEN date(payment_date) <= DATE('2023-11-30') AND lower(payment_status) = 'failed'
        THEN 'Manual'
        ELSE payment_status
    END payment_status,
    bank_name,
    company_name,
    reference,
    narration
FROM 
    payments_with_usd)

select 
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
    task_influencer_type,
    payment_eligible_at,
    payment_eligible_at_date_weekday_name,
    CASE
        WHEN payment_eligible_at_date_weekday_name IN('Mon','Tue','Wed','Fri', 'Sat', 'Sun') THEN DATE_ADD(DATE_TRUNC(DATE(payment_eligible_at), WEEK(THURSDAY)), INTERVAL 1 WEEK)
        WHEN payment_eligible_at_date_weekday_name IN('Thu') AND EXTRACT(hour FROM payment_eligible_at)>=17 THEN DATE_ADD(DATE_TRUNC(DATE(payment_eligible_at), WEEK(THURSDAY)), INTERVAL 1 WEEK)
    END expected_payment_date,
    payment_date,
    updated_at,
    country,
    amount,
    amount_usd,
    currency,
    exchange_rate,
    payment_status,
    bank_name,
    company_name,
    reference,
    narration
from dims_payments