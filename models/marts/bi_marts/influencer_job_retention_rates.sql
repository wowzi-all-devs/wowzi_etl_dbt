WITH cohort_items as 
(
SELECT
    MIN(cohort_month) cohort_month,
  influencer_id
FROM
(
SELECT  
  influencer_id,
  CAST(date_trunc(DATE(job_offer_date), MONTH) AS DATE) cohort_month
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown` 
  WHERE influencer_id IS NOT NULL
  AND job_status IN ('Complete', 'Ongoing')
) 
  GROUP BY influencer_id
  ORDER BY cohort_month, influencer_id
),

influencer_activities AS 
(
SELECT
  DISTINCT 
  a.influencer_id,
  a.job_offer_month,
  DATE_DIFF(a.job_offer_month, b.cohort_month, MONTH) month_number
FROM
(
SELECT 
    a.influencer_id,
    date_trunc(DATE(job_offer_date), MONTH) job_offer_month
FROM bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown a
  WHERE influencer_id IS NOT NULL
  AND job_status IN ('Complete', 'Ongoing')
) a
LEFT JOIN cohort_items b ON a.influencer_id = b.influencer_id
),

cohort_size AS 
(
SELECT 
    a.cohort_month,
    COUNT(a.influencer_id) num_influencers
FROM cohort_items a
    GROUP BY cohort_month
    ORDER BY cohort_month
),

retention_table1 AS
(
SELECT
    b.cohort_month,
    a.month_number,
    count(a.influencer_id) num_influenccers
FROM influencer_activities a 
LEFT JOIN cohort_items b ON a.influencer_id =b.influencer_id
    GROUP BY b.cohort_month, a.month_number
),

retention_table2 AS
(
SELECT
    b.cohort_month,
    a.month_number,
    a.job_offer_month active_month
FROM influencer_activities a 
LEFT JOIN cohort_items b ON a.influencer_id =b.influencer_id
),

retention_table AS 
(
SELECT 
  a.cohort_month,
  a.month_number,
  b.active_month,
  a.num_influenccers
FROM retention_table1 a 
LEFT JOIN retention_table2 b ON a.cohort_month = b.cohort_month
AND a.month_number = b.month_number
)

SELECT 
    DISTINCT
    a.cohort_month,
    CONCAT( FORMAT_DATETIME("%b", a.cohort_month ),"-", extract(YEAR FROM a.cohort_month) ) ||' '||'('||b.num_influencers||')' cohort_mon_yr,
    FORMAT_DATETIME("%b", DATETIME(DATE(a.cohort_month))) cohort_mon,
    EXTRACT(YEAR FROM a.cohort_month) cohort_yr,
    DENSE_RANK () OVER (ORDER BY extract(YEAR FROM a.cohort_month) ASC, EXTRACT(MONTH FROM a.cohort_month)ASC ) cohort_yr_rnk,
    b.num_influencers total_inf,
    a.month_number,
    a.active_month,
    a.num_influenccers active_inf,
    CAST(a.num_influenccers AS NUMERIC)/b.num_influencers retention_rate
FROM retention_table a
LEFT JOIN cohort_size b ON a.cohort_month = b.cohort_month
    WHERE a.cohort_month IS NOT NULL
    ORDER BY a.cohort_month, a.month_number
