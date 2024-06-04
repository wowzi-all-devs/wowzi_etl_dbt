WITH job_breakdwon AS
(SELECT 
    influencer_id,
    country,
    extract(year from job_offer_date) yr,
    CONCAT(
    CAST(EXTRACT(YEAR FROM date(job_offer_date)) AS STRING),
    ' Q', EXTRACT(QUARTER FROM date(job_offer_date))) AS quarter,
    extract(quarter from job_offer_date) quarter_no,
    dense_rank () over (order by extract(year from job_offer_date) asc, extract(quarter from job_offer_date)asc ) qtr_yr_rnk,
    influencer_level,
    amount_usd,
    payment_amount_list_usd,
    datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown`
where lower(job_status) in ('complete', 'ongoing')),

quarter_aggregates AS
(select 
    influencer_id,
    country,
    yr,
    quarter,
    quarter_no,
    qtr_yr_rnk,
    sum(amount_usd) amount_usd,
    sum(payment_amount_list_usd) payment_amount_list_usd
from job_breakdwon
    group by influencer_id, country, yr, quarter, quarter_no, qtr_yr_rnk
)

SELECT
    influencer_id,
    country,
    yr,
    quarter,
    quarter_no,
    qtr_yr_rnk,
    amount_usd,
    payment_amount_list_usd,
    case
    when 
    round(payment_amount_list_usd,0) <= 50 then '$50 and Below'
    when 
    round(payment_amount_list_usd,0) > 50  and round(payment_amount_list_usd,0) <= 100 then '$51 - $100'
    when 
    round(payment_amount_list_usd,0) >= 101 and round(payment_amount_list_usd,0) <= 500 then '$101 - $500'
    when 
    round(payment_amount_list_usd,0) >= 501 and round(payment_amount_list_usd,0) <= 1000 then '$501 - $1000'
    when 
    round(payment_amount_list_usd,0) > 1000 and round(payment_amount_list_usd,0) <= 10000 then '$1001 - $10000' 
    when 
    round(payment_amount_list_usd,0) >= 10000 then 'More than $10000' 
    else 'Uncategorized' 
    end as Income_bucket
FROM quarter_aggregates