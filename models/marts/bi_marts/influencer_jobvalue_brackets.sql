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
    amount_usd > 0 and amount_usd <= 100 then '$100 and Below'
    when 
    amount_usd >= 101 and amount_usd <= 500 then '$101 - $500'
    when 
    amount_usd >= 501 and amount_usd <= 1000 then '$501 - $1000'
    when 
    amount_usd > 1000  then 'More than $1000' else 'Uncategorized' 
    end as Income_bucket,
    datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown`)

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