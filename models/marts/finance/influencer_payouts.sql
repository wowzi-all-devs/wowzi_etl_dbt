{{ config(tags=["finance"]) }}

WITH dims_payments AS (
SELECT      
    {{ dbt_utils.surrogate_key(['inf_transfers.id', 'inf_transfers.influencer_id', 'inf_tasks.campaign_id', 'inf_transfers.task_id']) }} as primary_key,
    inf_transfers.influencer_id as influencer_id,
    inf.mobile_number,
    inf_tasks.campaign_id,
    companies.name as company_name,
    adv.advertiser_id,
    adv.first_name as advertiser_fname,
    adv.last_name as advertiser_lname,
    campaigns.name as campaign_name,
    inf_transfers.task_id,
    inf_transfers.amount,
    inf_transfers.status as payment_status,
    inf_transfers.date_created as payment_date,
    inf_transfers.currency as currency,
    inf.country,
    bd.bank_name as bank_name,
    bd.bank_account_number,
    inf_transfers.narration,
    inf_transfers.reference
FROM {{ ref('influencer_transfers') }} inf_transfers
   LEFT JOIN {{ ref('influencers') }} inf USING (influencer_id)
   LEFT JOIN {{ ref('bank_details') }} bd USING (influencer_id)
   LEFT JOIN {{ ref('influencer_tasks') }} inf_tasks ON inf_tasks.id = inf_transfers.task_id
   LEFT JOIN {{ ref('campaigns') }} campaigns ON campaigns.id=inf_tasks.campaign_id
   LEFT join {{ ref('dims_advertisers') }} adv on adv.advertiser_id=campaigns.merchant_id
   LEFT join {{ ref('companies') }} companies on companies.id = campaigns.company_id
)
SELECT
    primary_key,
    influencer_id,
    mobile_number,
    advertiser_id,
    campaign_id,
    task_id,
    campaign_name,
    payment_date,
    country,
    amount,
    currency,
    payment_status,
    bank_name,
    company_name 
FROM 
    dims_payments