WITH first_campaign AS 
(SELECT  
  merchant_id, merchant_name, company_name, 
  min(date(campaign_start_date)) as first_campaign_start_date
FROM {{ ref('campaign_expenditure') }} 
  where lower(campaign_name) not like '%test%'
  and budget_spent > 0
  and merchant_id <> 7
  and lower(company_name) not like '%wowzi%'
  group by merchant_id, merchant_name, company_name),

campaign_months AS
(select 
  merchant_id,company_name, count(distinct months) as no_of_months_with_campaigns_started
from
(SELECT 
  merchant_id, merchant_name, company_name,
  DATE_TRUNC(date(campaign_start_date), month) as months
FROM {{ ref('campaign_expenditure') }} 
  where lower(campaign_name) not like '%test%'
  and budget_spent > 0
  and merchant_id <> 7
  and lower(company_name) not like '%wowzi%') a
  group by merchant_id, company_name),

last_campaign AS
(SELECT  
  merchant_id, merchant_name, company_name, 
  max(date(campaign_start_date)) as campaign_start_date
FROM {{ ref('campaign_expenditure') }} 
  where lower(campaign_name) not like '%test%'
  and budget_spent > 0
  and merchant_id <> 7
  and lower(company_name) not like '%wowzi%'
  group by merchant_id, merchant_name, company_name),

campaign_budget AS
(SELECT 
  merchant_id, merchant_name, company_name,
  sum(budget_spent) as budget_spent
FROM
(SELECT 
  merchant_id, merchant_name, company_name, date(campaign_start_date) as campaign_start_date,
  ce.currency, budget_spent/cr.currency_rate as budget_spent
FROM {{ ref('campaign_expenditure') }}  ce
LEFT JOIN {{ ref('int_currency_rates') }} cr on upper(ce.currency) = upper(cr.currency)
and date(campaign_start_date) = date(cr.date)
  where lower(campaign_name) not like '%test%'
  and budget_spent > 0
  and merchant_id <> 7
  and lower(company_name) not like '%wowzi%') budgets_usd
  group by merchant_id, merchant_name, company_name),

campaign_rm AS
(SELECT 
   c.merchant_id, c.merchant_name, c.company_name,
   campaign_start_date as latest_campaign_start_date,
   DATE_DIFF(current_date, c.campaign_start_date, day) as R,
   cb.budget_spent as M, cm.no_of_months_with_campaigns_started as F
  FROM last_campaign c 
  LEFT JOIN campaign_budget cb ON c.merchant_id = cb.merchant_id
  AND c.company_name = cb.company_name
  LEFT JOIN campaign_months cm ON c.merchant_id = cm.merchant_id
  AND c.company_name = cm.company_name),  

campaign_scores AS
(SELECT 
    crm.merchant_id,
    crm.merchant_name,
    crm.company_name,
    crm.latest_campaign_start_date,
    crm.R,
    crm.F,
    crm.M
    --NTILE(10) OVER (ORDER BY R DESC) as R_S,
    --NTILE(10) OVER (ORDER BY F ASC) as F_S,
    --NTILE(10) OVER (ORDER BY M ASC) as M_S
FROM campaign_rm crm),

max_initial_scores AS 
(SELECT 
  max(R) AS R,
  max(F) AS F,
  max(M) AS M
FROm campaign_scores),

campaign_scores_2 AS
(SELECT 
    c.merchant_id,
    c.merchant_name,
    c.company_name,
    c.latest_campaign_start_date,
    c.R,
    c.F,
    c.M,
    10 - ((10/m.R)*c.R) AS R_S,
    (10/m.F)*c.F AS F_S,
    (10/m.M)*c.M AS M_S,
    (10 - ((10/m.R)*c.R))+((10/m.F)*c.F)+((10/m.M)*c.M) AS T
FROM campaign_scores c 
LEFT JOIN max_initial_scores m ON m.R = m.R),

max_main_score AS
(SELECT 
  max(T) AS T
FROM campaign_scores_2),

campaign_final_scores AS 
(SELECT 
  c.merchant_id,
  c.merchant_name,
  c.company_name,
  c.latest_campaign_start_date,
  c.R,
  c.F,
  c.M,
  c.R_S,
  c.F_S,
  c.M_S,
  c.T,
  (10/m.T)*c.T AS T_S
FROM campaign_scores_2 c
LEFT JOIN max_main_score m on m.T = m.T)

SELECT 
    cfs.merchant_id,
    cfs.merchant_name,
    cfs.company_name,
    fc.first_campaign_start_date,
    cfs.latest_campaign_start_date,
    cfs.R as days_since_last_campaign,
    cfs.F as no_of_months_with_campaigns_started,
    cfs.M as total_budget_spend_usd,
    cfs.R_S as recency_score,
    cfs.F_S as frequency_score,
    cfs.M_S as budget_spend_score,
    cfs.T_S as total_score,
    (CASE 
        WHEN cfs.T_S >= 8
        THEN 'Super Advertiser'
        WHEN cfs.T_S >= 6 AND cfs.T_S < 8
        THEN 'Dependable Advertiser'
        WHEN cfs.T_S >= 4 AND  cfs.T_S <= 6
        THEN 'Potential Dependable Advertiser'
        WHEN cfs.T_S >=3 AND cfs.T_S < 4
        AND cfs.R < 90
        THEN 'Need Attention'
        WHEN cfs.T_S >= 3 AND cfs.T_S < 4
        AND cfs.R >= 90
        THEN 'At Risk/Churned'
        WHEN cfs.R >= 90
        THEN 'At Risk/Churned'
        WHEN cfs.T_S <=3
        THEN 'At Risk/Churned'
    END) as company_segment,
    (CASE 
    WHEN cfs.R < 30 
    THEN 'Less than 30 Days'
    WHEN cfs.R >= 30 
    AND cfs.R < 60
    THEN '30 to 59 Days'
    WHEN cfs.R >= 60 
    AND cfs.R < 90
    THEN '60 to 89 Days'
    WHEN cfs.R >= 90 
    AND cfs.R <= 120
    THEN '90 to 120 Days'
    ELSE 'More than 120 Days'
    END) as days_since_last_campaign_start_brackets,
    (CASE 
    WHEN cfs.M >= 500000
    THEN '$500,000 +'
    WHEN cfs.M >= 300000 AND cfs.M < 500000
    THEN '$300,000 to $499,999'
    WHEN cfs.M >= 100000 AND cfs.M < 300000
    THEN '$100,000 to $299,999'
    WHEN cfs.M >= 50000 AND cfs.M < 100000
    THEN '$50,000 to $99,999'
    WHEN cfs.M >= 25000 AND cfs.M < 50000
    THEN '$25,000 to $49,999'
    WHEN cfs.M < 25000 
    THEN 'Less than $25,000'
    END) as budget_spend_brackets
FROM campaign_final_scores cfs
LEFT JOIN first_campaign fc on cfs.merchant_id = fc.merchant_id