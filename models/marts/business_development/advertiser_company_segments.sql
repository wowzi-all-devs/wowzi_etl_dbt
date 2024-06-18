WITh merchants AS 
(SELECT  
  distinct merchant_id, merchant_name, company_name
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_expenditure` 
  where merchant_id in  
  (select
  first_merchant
from
(SELECT 
  company_name, 
  min(merchant_id) as first_merchant
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_expenditure` 
  where lower(campaign_name) not like '%test%'
  and budget_spent > 0
  and merchant_id <> 7
  and merchant_id <> 8
  and lower(company_name) not like '%wowzi%'
  and company_name <> 'Advertiser Demo Account '
  group by company_name))),

first_campaign AS 
(SELECT 
  company_name, 
  min(date(campaign_start_date)) as first_campaign_start_date 
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_expenditure` 
  where lower(campaign_name) not like '%test%'
  and budget_spent > 0
  and merchant_id <> 7
  and lower(company_name) not like '%wowzi%'
  and company_name <> 'Advertiser Demo Account '
  group by company_name),

last_campaign AS 
(SELECT 
  company_name, 
  max(date(campaign_start_date)) as last_campaign_start_date 
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_expenditure` 
  where lower(campaign_name) not like '%test%'
  and budget_spent > 0
  and merchant_id <> 7
  and merchant_id <> 8
  and lower(company_name) not like '%wowzi%'
  and company_name <> 'Advertiser Demo Account '
  group by company_name),

campaign_months AS 
(SELECT 
  a.company_name,
  count(distinct a.months) as no_of_months_with_campaigns_started
from
(SELECT 
  company_name, 
  DATE_TRUNC(date(campaign_start_date), month) as months
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_expenditure` 
  where lower(campaign_name) not like '%test%'
  and budget_spent > 0
  and merchant_id <> 7
  and lower(company_name) not like '%wowzi%'
  and company_name <> 'Advertiser Demo Account ') a
  group by a.company_name),

campaign_budgets AS 
(select 
  company_name,
  sum(budget_spent) as budget_spent
from
(SELECT  
  company_name, 
  date(campaign_start_date) as campaign_start_date,
  ce.currency, budget_spent/cr.currency_rate as budget_spent
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_expenditure` ce 
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.currency_rates` cr ON upper(ce.currency) = upper(cr.currency)
and date(campaign_start_date) = date(cr.date)
  where lower(campaign_name) not like '%test%'
  and budget_spent > 0
  and merchant_id <> 7
  and lower(company_name) not like '%wowzi%'
  and company_name <> 'Advertiser Demo Account ') budget
  group by company_name),

campaign_rm AS
(SELECT 
  m.merchant_id, m.merchant_name,
  fc.company_name,
  fc.first_campaign_start_date,
  lc.last_campaign_start_date AS latest_campaign_start_date,
  DATE_DIFF(current_date, lc.last_campaign_start_date, day) AS R,
  cm.no_of_months_with_campaigns_started AS F,
  cb.budget_spent AS M
FROM first_campaign fc 
LEFT JOIN merchants m ON fc.company_name = m.company_name
LEFT JOIN last_campaign lc ON fc.company_name = lc.company_name
LEFT JOIN campaign_months cm ON fc.company_name = cm.company_name
LEFT JOIN campaign_budgets cb ON fc.company_name = cb.company_name),

max_scores AS
(SELECT 
  max(R) AS max_R,
  max(F) AS max_F,
  max(M) AS max_M
FROM campaign_rm),

campaign_scores AS
(SELECT
  crm.merchant_id, 
  crm.merchant_name,
  crm.company_name,
  crm.first_campaign_start_date,
  crm.latest_campaign_start_date,
  crm.R,
  crm.F,
  crm.M,
  10 - ((10/ms.max_R)*crm.R) AS R_S,
  (10/ms.max_F)*crm.F AS F_S,
  (10/ms.max_M)*crm.M AS M_S,
  (10 - ((10/ms.max_R)*crm.R))+((10/ms.max_F)*crm.F)+((10/ms.max_M)*crm.M) AS T
FROM campaign_rm crm
LEFT JOIN max_scores ms ON ms.max_R = ms.max_R),

max_main_score AS 
(SELECT
  max(T) AS max_T
FROM campaign_scores),

campaign_final_scores AS
(SELECT 
  cs.merchant_id, 
  cs.merchant_name,
  cs.company_name,
  cs.first_campaign_start_date,
  cs.latest_campaign_start_date,
  cs.R,
  cs.F,
  cs.M,
  cs.R_S,
  cs.F_S,
  cs.M_S,
  cs.T,
  (10/ms.max_T)*cs.T AS T_S
FROM campaign_scores cs
LEFT JOIN max_main_score ms ON ms.max_T = ms.max_T)

SELECT 
  cfs.merchant_id, 
  cfs.merchant_name,
  cfs.company_name,
  a.email,
  a.phone,
  cfs.first_campaign_start_date,
  cfs.latest_campaign_start_date,
  cfs.R AS days_since_last_campaign,
  cfs.F AS no_of_months_with_campaigns_started,
  cfs.M AS total_budget_spend_usd,
  cfs.R_S AS recency_score,
  cfs.F_S AS frequency_score,
  cfs.M_S AS budget_spend_score,
  cfs.T_S AS total_score,
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
left join `bi-staging-1-309112.wowzi_dbt_prod.dim_advertisers` a 
on cfs.merchant_id = a.Advertiser_id