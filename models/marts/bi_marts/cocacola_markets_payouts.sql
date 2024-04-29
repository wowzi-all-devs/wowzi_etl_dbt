WITH platform_payments AS
(SELECT  
  transfer_id,
  influencer_id,
  influencer_name,
  mobile_number,
  gender,
  advertiser_id,
  campaign_id,
  task_id,
  campaign_name,
  lower(REGEXP_REPLACE(campaign_name, r'[^a-zA-Z0-9]', '')) clean_campaign_name,
  social_media_channel,
  task_influencer_type,
  payment_date,
  INITCAP(c.country) country,
  amount,
  amount_usd,
  currency,
  payment_status,
  bank_name,
  company_name,
  'Coca Cola' as client,
  CASE 
    WHEN REGEXP_CONTAINS(lower(campaign_name), r'\b(coke|sprite|keringet|cappy|minute maid|fanta|schweppes)\b') 
    THEN INITCAP(REGEXP_EXTRACT(lower(campaign_name), r'\b(coke|sprite|keringet|cappy|minute maid|fanta|schweppes)\b'))
    WHEN REGEXP_CONTAINS(lower(campaign_name), r'\b(cokewithmeals)\b') 
    THEN 'Coke'
    WHEN REGEXP_CONTAINS(lower(campaign_name), r'\b(coca-cola)\b') 
    THEN 'Coke'
    WHEN REGEXP_CONTAINS(lower(campaign_name), r'\b(coca cola)\b') 
    THEN 'Coke'
    ELSE NULL
  END AS brand, 
  reference,
  narration,
  'Platform' as datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_payouts` p
left join bi-staging-1-309112.wowzi_dbt_prod.country_key c
on (lower(p.country) = lower(c.Key))
  where lower(REGEXP_REPLACE(company_name, r'[^a-zA-Z0-9]', '')) like '%cocacola%'),
/*
max_transfer_id as 
(SELECT  
  max(transfer_id) + 1000 max_transfer_id
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_payouts` ),
*/

periphery_payments AS
(SELECT  
    null transfer_id,
    influencer_id,
    influencer influencer_name,
    null mobile_number,
    gender,
    null advertiser_id,
    campaign_id,
    task_id,
    campaign_name,
    social_media_platform social_media_channel,
    influencer_level task_influencer_type,
    payment_date,
    country,
    amount_lcy amount,
    amount_usd,
    currency,
    payment_status,
    bank bank_name,
    null company_name,
    client,
    brand,
    null reference,
    null narration,
    'Periphery Sheet' as datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data_clean`),

all_payments as
(select 
    transfer_id,
    influencer_id,
    influencer_name,
    gender,
    campaign_id,
    task_id,
    campaign_name,
    social_media_channel,
    task_influencer_type,
    payment_date,
    country,
    amount,
    amount_usd,
    currency,
    case when lower(payment_status) in ('successful', 'manual', 'new', 'completed')
    then 'Successful'
    else 'Pending'
    end payment_status,
    bank_name,
    company_name,
    client,
    brand,
    datasource
from platform_payments
union all 
select 
    transfer_id,
    influencer_id,
    influencer_name,
    gender,
    campaign_id,
    task_id,
    campaign_name,
    social_media_channel,
    task_influencer_type,
    payment_date,
    country,
    amount,
    amount_usd,
    currency,
    payment_status,
    bank_name,
    null company_name,
    client,
    brand,
    datasource
from periphery_payments)


SELECT 
    a.transfer_id,
    a.influencer_id,
    a.influencer_name,
    a.gender,
    a.campaign_id,
    a.task_id,
    a.campaign_name,
    a.social_media_channel,
    a.task_influencer_type,
    a.payment_date,
    concat(FORMAT_DATETIME("%b", DATETIME(date(payment_date))),"-", extract(year from date(payment_date))) mon_yr,
    dense_rank () over (order by extract(year from payment_date) asc, extract(month from payment_date)asc ) mon_yr_rnk,
    a.country,
    a.amount,
    a.amount_usd,
    a.currency,
    a.payment_status,
    a.bank_name,
    a.company_name,
    a.client,
    a.brand,
    a.datasource
FROM all_payments a 