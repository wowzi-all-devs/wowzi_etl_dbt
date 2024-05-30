WITH job_breakdwon AS
(SELECT 
    influencer_id,
    country,
    extract(month from job_offer_date) mon_no,
    FORMAT_DATETIME("%b", DATETIME(date(job_offer_date))) mon,
    extract(year from job_offer_date) yr,
    mon_yr,
    influencer_level,
    amount_usd,
    case
    when 
    payment_amount_list_usd <= 50 then '$50 and Below'
    when 
    payment_amount_list_usd > 50  and payment_amount_list_usd <= 100 then '$51 - $100'
    when 
    payment_amount_list_usd >= 101 and payment_amount_list_usd <= 500 then '$101 - $500'
    when 
    payment_amount_list_usd >= 501 and payment_amount_list_usd <= 1000 then '$501 - $1000'
    when 
    payment_amount_list_usd > 1000 and payment_amount_list_usd <= 10000 then '$1001 - $10000' 
    when 
    payment_amount_list_usd >= 10000 then 'More than $10000' 
    else 'Uncategorized' 
    end as Income_bucket,
    datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown`
where lower(job_status) in ('complete', 'ongoing'))

select 
    influencer_id,
    country,
    mon_no,
    mon,
    yr,
    mon_yr,
    influencer_level,
    amount_usd,
    Income_bucket,
    datasource
from job_breakdwon