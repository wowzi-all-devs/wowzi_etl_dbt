WITH job_breakdown AS
(
SELECT 
    influencer_id,
    gender,
    country,
    extract(YEAR FROM job_offer_date) yr,
    CONCAT(
    CAST(EXTRACT(YEAR FROM date(job_offer_date)) AS STRING),
    ' Q', EXTRACT(QUARTER FROM date(job_offer_date))) AS quarter,
    extract(quarter from job_offer_date) quarter_no,
    dense_rank () OVER (ORDER BY extract(YEAR FROM job_offer_date) ASC, extract(QUARTER FROM job_offer_date)ASC ) qtr_yr_rnk,
    influencer_level,
    amount_usd,
    payment_amount_list_usd,
    datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown`
    WHERE lower(job_status) IN ('complete', 'ongoing')
),

quarter_aggregates AS
(
SELECT 
    influencer_id,
    gender,
    country,
    yr,
    quarter,
    quarter_no,
    qtr_yr_rnk,
    sum(amount_usd) amount_usd,
    sum(payment_amount_list_usd) payment_amount_list_usd
FROM job_breakdown
    GROUP BY influencer_id, gender, country, yr, quarter, quarter_no, qtr_yr_rnk
)

SELECT
    influencer_id,
    gender,
    country,
    yr,
    quarter,
    quarter_no,
    qtr_yr_rnk,
    amount_usd,
    payment_amount_list_usd,
    CASE
        WHEN round(payment_amount_list_usd,0) <= 50 
            THEN '$50 and Below'
        WHEN round(payment_amount_list_usd,0) > 50  AND round(payment_amount_list_usd,0) <= 100 
            THEN '$51 - $100'
        WHEN round(payment_amount_list_usd,0) >= 101 AND round(payment_amount_list_usd,0) <= 500 
            THEN '$101 - $500'
        WHEN round(payment_amount_list_usd,0) >= 501 AND round(payment_amount_list_usd,0) <= 1000 
            THEN '$501 - $1000'
        WHEN round(payment_amount_list_usd,0) > 1000 AND round(payment_amount_list_usd,0) <= 10000 
            THEN '$1001 - $10000' 
        WHEN round(payment_amount_list_usd,0) >= 10000 
            THEN 'More than $10000' 
        ELSE 'Uncategorized' 
    END AS Income_bucket
FROM quarter_aggregates