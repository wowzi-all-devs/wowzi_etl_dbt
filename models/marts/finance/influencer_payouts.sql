{{ config(tags=["finance"]) }}

WITH dims_payments AS (
SELECT      
    inf_transfers.id as transfer_id,
    inf_transfers.influencer_id,
    inf.mobile_number,
    inf_tasks.campaign_id,
    companies.company_name,
    adv.advertiser_id,
    adv.first_name as advertiser_fname,
    adv.last_name as advertiser_lname,
    campaigns.campaign_name,
    inf_transfers.task_id,
    inf_transfers.amount,
    inf_transfers.status as payment_status,
    date(inf_transfers.date_created) as payment_date,
    inf_transfers.currency as currency,
    inf.country,
    bd.bank_name as bank_name,
    bd.bank_account_number,
    inf_transfers.narration,
    inf_transfers.reference
FROM {{ ref('postgres_stg__influencer_transfers') }} inf_transfers
   LEFT JOIN {{ ref('postgres_stg__influencers') }} inf USING (influencer_id)
   LEFT JOIN {{ ref('postgres_stg__bank_details') }} bd USING (influencer_id)
   LEFT JOIN {{ ref('postgres_stg__influencer_tasks') }} inf_tasks ON inf_tasks.task_id = inf_transfers.task_id
   LEFT JOIN {{ ref('postgres_stg__campaigns') }} campaigns ON campaigns.campaign_id=inf_tasks.campaign_id
   LEFT join {{ ref('postgres_stg__merchants') }} adv on adv.advertiser_id=campaigns.merchant_id
   LEFT join {{ ref('postgres_stg__companies') }} companies on companies.company_id = campaigns.company_id
)
SELECT
    transfer_id,
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
    company_name,
    reference,
    narration
FROM 
    dims_payments