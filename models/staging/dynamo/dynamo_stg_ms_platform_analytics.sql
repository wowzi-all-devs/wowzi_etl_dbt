
with a as 
(
select 
  event_id,
  JSON_VALUE(metadata, '$.user_id') AS user_id,
  date(created) created,
  JSON_VALUE(tracking, '$.decoded_ip_info.country') AS country,
  JSON_VALUE(tracking, '$.decoded_ip_info.city') AS city,
  JSON_VALUE(tracking, '$.decoded_ip_info.org') AS org,

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
  {{ source('staging', 'ms_platform_analytics') }} 
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
  REGEXP_REPLACE(a.org, r'^AS\d+\s*', '') AS org,
  a.last_24_hours_activity,
  a.last_7_days_activity,
  a.last_1_month_activity
  from 
  a LEFT join   
  country
  on a.country = country.Key


  