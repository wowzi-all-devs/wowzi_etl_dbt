WITH monthly_brackets AS 
(
    SELECT 
        yr,
        quarter_no,
        qtr_yr_rnk,
        quarter,
        Income_bucket,
        count(distinct influencer_id) no_influencers
    FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_jobvalue_quarterly_brackets` 
        group by yr,
        quarter_no,
        qtr_yr_rnk,
        quarter,
        Income_bucket
),

first_dates AS 
(
    SELECT 
        Income_bucket,
        min(yr) first_yr,
        min(quarter_no) first_quarter_no,
        min(qtr_yr_rnk) first_qtr_yr_rnk
    FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_jobvalue_quarterly_brackets` 
        group by Income_bucket
),

initial_number as 
(
    SELECT 
        m.yr,
        m.quarter_no,
        m.qtr_yr_rnk,
        m.quarter,
        m.Income_bucket,
        m.no_influencers,
        m.qtr_yr_rnk - f.first_qtr_yr_rnk month_no
    FROM monthly_brackets m 
    join first_dates f 
    ON m.Income_bucket = f.Income_bucket
    and m.qtr_yr_rnk = f.first_qtr_yr_rnk
),

retention_rates as
(
    SELECT 
        m.yr,
        m.quarter_no,
        m.qtr_yr_rnk,
        m.quarter,
        m.Income_bucket,
        m.no_influencers,
        LAG(m.no_influencers, 1) OVER (PARTITION BY m.Income_bucket ORDER BY m.qtr_yr_rnk) AS prev_no_influencers,
        im.no_influencers initial_influencer,
        m.qtr_yr_rnk - f.first_qtr_yr_rnk month_no,
        m.no_influencers/im.no_influencers retention_rate
    FROM monthly_brackets m 
    left join first_dates f 
    ON m.Income_bucket = f.Income_bucket
    left join initial_number im 
    on m.Income_bucket = im.Income_bucket
)    

select
    yr,
    quarter_no,
    qtr_yr_rnk,
    quarter,
    Income_bucket,
    no_influencers,
    prev_no_influencers,
    initial_influencer,
    month_no,
    retention_rate,
    case 
        when prev_no_influencers is null then 0
        when no_influencers is null then 0
        when prev_no_influencers = 0 then 1
        else (no_influencers - prev_no_influencers)/prev_no_influencers
    end mom_growth
from retention_rates
    order by Income_bucket, qtr_yr_rnk

