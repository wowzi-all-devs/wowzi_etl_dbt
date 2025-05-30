select 
  event_id,
  date(created) created,
  JSON_VALUE(tracking, '$.decoded_ip_info.city') AS city,
  JSON_VALUE(tracking, '$.decoded_ip_info.country') AS country,
  platform,
--   event_name,
  REGEXP_REPLACE(event_name, r'([a-z])([A-Z])', r'\1 \2') AS fine_event_name -- This regex replaces camelCase with space-separated words
from 
  {{ source('staging', 'ms_platform_analytics') }}
