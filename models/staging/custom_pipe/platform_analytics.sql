
with a as 
(
select 
  event_id,
  user_id,
  date(created) created,
  country,
  city,
  org,

CASE 
  WHEN Date(created) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  THEN TRUE 
  ELSE FALSE
END AS last_24_hours_activity,

CASE 
  WHEN DATE(created) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
  THEN TRUE 
  ELSE FALSE
END AS last_7_days_activity,

CASE 
  WHEN DATE(created) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AND CURRENT_DATE()
  THEN TRUE 
  ELSE FALSE 
END AS last_1_month_activity,

  platform,
--   event_name,
  REGEXP_REPLACE(event_name, r'([a-z])([A-Z])', r'\1 \2') AS fine_event_name -- This regex replaces camelCase with space-separated words
from 
  {{ source('staging', 'platform_analytics') }} 
),
country as 
(
SELECT * 
FROM bi-staging-1-309112.dbt_kayode.country_key 
)
SELECT 
  a.event_id,
  SAFE_CAST(a.user_id AS INT64) AS user_id,
  a.created,
  a.city,
  country.Country AS country,
  a.platform,
  a.fine_event_name,
    case when
  a.fine_event_name = 'Landing Page Viewed' then 'Login Viewed'
  when a.fine_event_name = 'Sign Up Clicked' then 'Sign-up Form Viewed'
  when a.fine_event_name = 'Sign Up' then 'Sign-up Submitted'
  when a.fine_event_name = 'User Login' then 'Login Successful'
  else NULL
  end as key_platform_event,
  a.org,
  a.last_24_hours_activity,
  a.last_7_days_activity,
  a.last_1_month_activity
  from 
  a LEFT join   
  country
  on a.country = country.Key


  