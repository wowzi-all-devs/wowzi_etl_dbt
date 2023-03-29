WITH last_campaign AS
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
   DATE_DIFF(current_date, c.campaign_start_date, day) as R,
   cb.budget_spent as M
  FROM last_campaign c 
  LEFT JOIN campaign_budget cb ON c.merchant_id = cb.merchant_id
  AND c.company_name = cb.company_name),  

campaign_scores AS
(SELECT 
    crm.merchant_id,
    crm.merchant_name,
    crm.company_name,
    crm.R,
    crm.M,
    NTILE(10) OVER (ORDER BY R DESC) as R_S,
    NTILE(10) OVER (ORDER BY M ASC) as M_S
FROM campaign_rm crm),

campaign_final_scores AS
(SELECT 
    merchant_id,
    merchant_name,
    company_name,
    R,
    M,
    R_S,
    M_S,
    R_S+M_S as T,
    NTILE(10) OVER (ORDER BY R_S+M_S ASC) as T_S
FROM campaign_scores)

SELECT 
    cfs.merchant_id,
    cfs.merchant_name,
    cfs.company_name,
    cfs.R as days_since_last_campaign,
    cfs.M as total_budget_spend_usd,
    cfs.R_S as recency_score,
    cfs.M_S as budget_spend_score,
    cfs.T_S as total_score,
    (CASE 
        WHEN cfs.T_S >=9
        THEN 'Super Advertiser'
        WHEN cfs.T_S >=6 AND cfs.T_S <9
        THEN 'Dependable Advertiser'
        WHEN cfs.T_S >=4 AND cfs.T_S <6
        THEN 'Need Attention'
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