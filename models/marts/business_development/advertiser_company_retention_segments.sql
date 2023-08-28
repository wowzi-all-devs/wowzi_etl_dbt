WITH distinct_campaign_months AS 
(SELECT 
  DISTINCT campaign_month
FROM
(SELECT  
  format_date('%Y-%m', start_date) AS campaign_month
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_facts`
  WHERE lower(company_name) NOT LIKE '%test%'
  AND lower(company_name) NOT LIKE '%wowzi%'
  AND company_name <> 'Advertiser Demo Account '
  AND canceled IS NULL
  AND budget_spent > 0)),

list_of_companies AS 
(SELECT  
  DISTINCT
  company_id,
  company_name
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_facts`
  WHERE lower(company_name) NOT LIKE '%test%'
  AND lower(company_name) NOT LIKE '%wowzi%'
  AND company_name <> 'Advertiser Demo Account '
  AND canceled IS NULL
  AND budget_spent > 0
  AND company_id IS NOT NULL),

company_campaign_months AS 
(SELECT
  DISTINCT 
  company_id, company_name, campaign_month
FROM
(SELECT  
  company_id,
  company_name,
  format_date('%Y-%m', start_date) AS campaign_month
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_facts`
  WHERE lower(company_name) NOT LIKE '%test%'
  AND lower(company_name) NOT LIKE '%wowzi%'
  AND company_name <> 'Advertiser Demo Account '
  AND canceled IS NULL
  AND budget_spent > 0
  AND company_id IS NOT NULL)),

all_companies_all_months AS 
(SELECT 
    d.campaign_month,
    c.company_id,
    c.company_name,
  FROM distinct_campaign_months d 
  LEFT JOIN  list_of_companies c ON d.campaign_month = d.campaign_month),

company_monthly_status AS
(SELECT 
    a.campaign_month,
    a.company_id,
    a.company_name,
    ccm.campaign_month AS active_month
FROM all_companies_all_months a 
LEFT JOIN company_campaign_months ccm ON a.campaign_month = ccm.campaign_month
AND a.company_id = ccm.company_id),

company_monthly_status_with_lag_lead AS 
(SELECT 
    m.campaign_month,
    m.company_id,
    m.company_name,
    m.active_month,
    lag(m.active_month,1) OVER (PARTITION BY m.company_id ORDER BY m.company_id, m.campaign_month) AS lag,
    lead(m.active_month,1) OVER (PARTITION BY m.company_id ORDER BY m.company_id, m.campaign_month) AS lead
FROM company_monthly_status m
ORDER BY m.company_id, m.campaign_month),

first_company_campaign_dates AS 
(SELECT  
  company_id,
  company_name,
  min(start_date) AS first_campaign_date
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_facts`
  WHERE lower(company_name) NOT LIKE '%test%'
  AND lower(company_name) NOT LIKE '%wowzi%'
  AND company_name <> 'Advertiser Demo Account '
  AND canceled IS NULL
  AND budget_spent > 0
  AND company_id IS NOT NULL
  GROUP BY company_id, company_name),

first_status AS 
(SELECT 
  company_id,
  format_date('%Y-%m', first_campaign_date) AS first_month,
  'NEW' AS first_status
FROM first_company_campaign_dates),

last_company_campaign_dates AS 
(SELECT  
  company_id,
  company_name,
  max(start_date) AS last_campaign_date
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_facts`
  WHERE lower(company_name) NOT LIKE '%test%'
  AND lower(company_name) NOT LIKE '%wowzi%'
  AND company_name <> 'Advertiser Demo Account '
  AND canceled IS NULL
  AND budget_spent > 0
  AND company_id IS NOT NULL
  GROUP BY company_id, company_name)

SELECT 
    r.campaign_month,
    r.company_id,
    r.company_name,
    r.active_month,
    r.lag,
    r.lead,
    fs.first_status,
    fs2.first_month,
    ls.last_campaign_date,
    (CASE 
    WHEN fs.first_status = 'NEW' THEN fs.first_status
    WHEN r.lag IS NOT NULL and r.active_month IS NOT NULL
    THEN 'RETAINED'
    WHEN r.active_month IS NULL
    AND r.campaign_month > fs2.first_month
    THEN 'CHURNED'
    WHEN r.active_month IS NOT NULL AND r.lag IS NULL AND fs.first_status IS NULL
    THEN 'RESSURRECTED'
    WHEN r.active_month IS NULL 
    AND r.campaign_month < fs2.first_month
    THEN 'INACTIVATED'
    --AND DATE_DIFF(current_date, ls.last_campaign_date, day) >= 120
    --THEN 'DORMANT'
    --WHEN r.lag IS NULL AND r.lead is null AND r.active_month IS NULL
    --AND r.campaign_month >= fs2.first_month
    --THEN 'CHURNED' 
   -- WHEN r.lag IS NOT NULL AND r.active_month IS NULL
    --THEN 'CHURNED' 
    END) AS retention_segment
FROM company_monthly_status_with_lag_lead r 
LEFT JOIN first_status fs ON r.company_id = fs.company_id
AND r.campaign_month = fs.first_month
LEFT JOIN first_status fs2 ON r.company_id = fs2.company_id
LEFT JOIN last_company_campaign_dates ls ON r.company_id = ls.company_id
ORDER BY r.company_id, r.campaign_month