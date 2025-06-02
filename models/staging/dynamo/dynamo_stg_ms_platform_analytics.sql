
with a as 
(
select 
  event_id,
  JSON_VALUE(metadata, '$.user_id') AS user_id,
  date(created) created,
  JSON_VALUE(tracking, '$.decoded_ip_info.country') AS country,
  JSON_VALUE(tracking, '$.decoded_ip_info.city') AS city,
  JSON_VALUE(tracking, '$.decoded_ip_info.org') AS org,

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
  REGEXP_REPLACE(a.org, r'^AS\d+\s*', '') AS org
  from 
  a LEFT join   
  country
  on a.country = country.Key


  