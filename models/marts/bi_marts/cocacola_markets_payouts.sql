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
  social_media_channel,
  task_influencer_type,
  payment_date,
  country,
  amount,
  amount_usd,
  currency,
  payment_status,
  bank_name,
  company_name,
  reference,
  narration,
  'Platform' as datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_payouts` 
  where lower(REGEXP_REPLACE(company_name, r'[^a-zA-Z0-9]', '')) like '%cocacola%'),

max_transfer_id as 
(SELECT  
  max(transfer_id) + 1000 max_transfer_id
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_payouts` ),

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
    null reference,
    null narration,
    'Periphery Sheet' as datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data_clean`)

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
    case when lower(payment_status) in ('successful', 'Manual', 'New', 'Completed')
    then 'Successful'
    else 'Pending'
    end payment_status,
    bank_name,
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
    datasource
from periphery_payments