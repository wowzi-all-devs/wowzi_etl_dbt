WITH app_launches AS 
(SELECT 
  ts as event_datecol,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)) AS date) AS event_date,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)||'T'||
  SUBSTRING(ts, 9, 2)||':'||SUBSTRING(ts, 11, 2)||':'||RIGHT(ts, 2)) AS datetime) AS event_datetime,
  eventName,
  cast(JSON_EXTRACT_SCALAR(profile, '$.identity') as int ) as profile_id,
  JSON_EXTRACT_SCALAR(profile, '$.name') as profile_name,
  JSON_EXTRACT_SCALAR(profile, '$.email') as profile_email,
  JSON_EXTRACT(profile, '$.all_identities') as profile_all_identities,
  JSON_EXTRACT_SCALAR(profile, '$.platform') as profile_platform,
  JSON_EXTRACT_SCALAR(profile, '$.push_token') as push_token,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.make') as device_make,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.model') as device_model,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.osVersion') as device_os_version,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.appVersion') as app_version
FROM {{ source('staging', 'App Launched') }} ),

app_installs AS 
(SELECT 
  ts as event_datecol,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)) AS date) AS event_date,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)||'T'||
  SUBSTRING(ts, 9, 2)||':'||SUBSTRING(ts, 11, 2)||':'||RIGHT(ts, 2)) AS datetime) AS event_datetime,
  eventName,
  cast(JSON_EXTRACT_SCALAR(profile, '$.identity') as int ) as profile_id,
  JSON_EXTRACT_SCALAR(profile, '$.name') as profile_name,
  JSON_EXTRACT_SCALAR(profile, '$.email') as profile_email,
  JSON_EXTRACT(profile, '$.all_identities') as profile_all_identities,
  JSON_EXTRACT_SCALAR(profile, '$.platform') as profile_platform,
  JSON_EXTRACT_SCALAR(profile, '$.push_token') as push_token,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.make') as device_make,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.model') as device_model,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.osVersion') as device_os_version,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.appVersion') as app_version
FROM {{ source('staging', 'App Installed') }} ),

app_uninstalls AS 
(SELECT  
  ts as event_datecol,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)) AS date) AS event_date,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)||'T'||
  SUBSTRING(ts, 9, 2)||':'||SUBSTRING(ts, 11, 2)||':'||RIGHT(ts, 2)) AS datetime) AS event_datetime,
  eventName,
  cast(JSON_EXTRACT_SCALAR(profile, '$.identity') as int ) as profile_id,
  JSON_EXTRACT_SCALAR(profile, '$.name') as profile_name,
  JSON_EXTRACT_SCALAR(profile, '$.email') as profile_email,
  JSON_EXTRACT(profile, '$.all_identities') as profile_all_identities,
  JSON_EXTRACT_SCALAR(profile, '$.platform') as profile_platform,
  JSON_EXTRACT_SCALAR(eventProps, '$.token') as push_token,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.make') as device_make,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.model') as device_model,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.osVersion') as device_os_version,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.appVersion') as app_version
FROM {{ source('staging', 'App Uninstalled') }} ),

login_method AS 
(SELECT  
  ts as event_datecol,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)) AS date) AS event_date,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)||'T'||
  SUBSTRING(ts, 9, 2)||':'||SUBSTRING(ts, 11, 2)||':'||RIGHT(ts, 2)) AS datetime) AS event_datetime,
  eventName,
  cast(JSON_EXTRACT_SCALAR(profile, '$.identity') as int ) as profile_id,
  JSON_EXTRACT_SCALAR(profile, '$.name') as profile_name,
  JSON_EXTRACT_SCALAR(profile, '$.email') as profile_email,
  JSON_EXTRACT(profile, '$.all_identities') as profile_all_identities,
  JSON_EXTRACT_SCALAR(profile, '$.platform') as profile_platform,
  JSON_EXTRACT_SCALAR(profile, '$.push_token') as push_token,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.make') as device_make,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.model') as device_model,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.osVersion') as device_os_version,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.appVersion') as app_version,
  JSON_EXTRACT_SCALAR(eventProps, '$.type') as login_type,
  JSON_EXTRACT_SCALAR(eventProps, '$.CT Session Id') as login_session_id
FROM {{ source('staging', 'LoginMethod') }} ),

login_with_apple AS 
(SELECT  
  ts as event_datecol,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)) AS date) AS event_date,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)||'T'||
  SUBSTRING(ts, 9, 2)||':'||SUBSTRING(ts, 11, 2)||':'||RIGHT(ts, 2)) AS datetime) AS event_datetime,
  eventName,
  cast(JSON_EXTRACT_SCALAR(profile, '$.identity') as int ) as profile_id,
  JSON_EXTRACT_SCALAR(profile, '$.name') as profile_name,
  JSON_EXTRACT_SCALAR(profile, '$.email') as profile_email,
  JSON_EXTRACT(profile, '$.all_identities') as profile_all_identities,
  JSON_EXTRACT_SCALAR(profile, '$.platform') as profile_platform,
  JSON_EXTRACT_SCALAR(profile, '$.push_token') as push_token,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.make') as device_make,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.model') as device_model,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.osVersion') as device_os_version,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.appVersion') as app_version,
  JSON_EXTRACT_SCALAR(eventProps, '$.type') as login_type,
  JSON_EXTRACT_SCALAR(eventProps, '$.CT Session Id') as login_session_id
FROM {{ source('staging', 'LoginWithApple') }} ),

login_with_facebook AS 
(SELECT  
  ts as event_datecol,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)) AS date) AS event_date,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)||'T'||
  SUBSTRING(ts, 9, 2)||':'||SUBSTRING(ts, 11, 2)||':'||RIGHT(ts, 2)) AS datetime) AS event_datetime,
  eventName,
  cast(JSON_EXTRACT_SCALAR(profile, '$.identity') as int ) as profile_id,
  JSON_EXTRACT_SCALAR(profile, '$.name') as profile_name,
  JSON_EXTRACT_SCALAR(profile, '$.email') as profile_email,
  JSON_EXTRACT(profile, '$.all_identities') as profile_all_identities,
  JSON_EXTRACT_SCALAR(profile, '$.platform') as profile_platform,
  JSON_EXTRACT_SCALAR(profile, '$.push_token') as push_token,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.make') as device_make,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.model') as device_model,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.osVersion') as device_os_version,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.appVersion') as app_version,
  JSON_EXTRACT_SCALAR(eventProps, '$.type') as login_type,
  JSON_EXTRACT_SCALAR(eventProps, '$.CT Session Id') as login_session_id
FROM {{ source('staging', 'LoginWithFacebook') }} ),

login_with_google AS 
(SELECT 
  ts as event_datecol,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)) AS date) AS event_date,
  CAST((SUBSTRING(ts, 1, 4)||'-'||SUBSTRING(ts, 5, 2)||'-'||SUBSTRING(ts, 7, 2)||'T'||
  SUBSTRING(ts, 9, 2)||':'||SUBSTRING(ts, 11, 2)||':'||RIGHT(ts, 2)) AS datetime) AS event_datetime,
  eventName,
  cast(JSON_EXTRACT_SCALAR(profile, '$.identity') as int ) as profile_id,
  JSON_EXTRACT_SCALAR(profile, '$.name') as profile_name,
  JSON_EXTRACT_SCALAR(profile, '$.email') as profile_email,
  JSON_EXTRACT(profile, '$.all_identities') as profile_all_identities,
  JSON_EXTRACT_SCALAR(profile, '$.platform') as profile_platform,
  JSON_EXTRACT_SCALAR(profile, '$.push_token') as push_token,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.make') as device_make,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.model') as device_model,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.osVersion') as device_os_version,
  JSON_EXTRACT_SCALAR(deviceinfo, '$.appVersion') as app_version,
  JSON_EXTRACT_SCALAR(eventProps, '$.type') as login_type,
  JSON_EXTRACT_SCALAR(eventProps, '$.CT Session Id') as login_session_id
FROM {{ source('staging', 'LoginWithGoogle') }} )

SELECT 
  event_datetime,
  eventName,
  profile_id,
  profile_name,
  profile_email,
  profile_all_identities,
  profile_platform,
  push_token,
  device_make,
  device_model,
  device_os_version,
  app_version,
  NULL AS login_type,
  NULL AS login_session_id
FROM app_launches al
UNION ALL 
SELECT
  event_datetime,
  eventName,
  profile_id,
  profile_name,
  profile_email,
  profile_all_identities,
  profile_platform,
  push_token,
  device_make,
  device_model,
  device_os_version,
  app_version,
  NULL AS login_type,
  NULL AS login_session_id
FROM app_installs ai
UNION ALL 
SELECT 
  event_datetime,
  eventName,
  profile_id,
  profile_name,
  profile_email,
  profile_all_identities,
  profile_platform,
  push_token,
  device_make,
  device_model,
  device_os_version,
  app_version,
  NULL AS login_type,
  NULL AS login_session_id
FROM app_uninstalls au
UNION ALL 
SELECT 
  event_datetime,
  eventName,
  profile_id,
  profile_name,
  profile_email,
  profile_all_identities,
  profile_platform,
  push_token,
  device_make,
  device_model,
  device_os_version,
  app_version,
  login_type,
  login_session_id
FROM login_method lm
UNION ALL 
SELECT 
  event_datetime,
  eventName,
  profile_id,
  profile_name,
  profile_email,
  profile_all_identities,
  profile_platform,
  push_token,
  device_make,
  device_model,
  device_os_version,
  app_version,
  login_type,
  login_session_id
FROM login_with_apple la
UNION ALL 
SELECT 
  event_datetime,
  eventName,
  profile_id,
  profile_name,
  profile_email,
  profile_all_identities,
  profile_platform,
  push_token,
  device_make,
  device_model,
  device_os_version,
  app_version,
  login_type,
  login_session_id
FROM login_with_facebook
UNION ALL 
SELECT 
  event_datetime,
  eventName,
  profile_id,
  profile_name,
  profile_email,
  profile_all_identities,
  profile_platform,
  push_token,
  device_make,
  device_model,
  device_os_version,
  app_version,
  login_type,
  login_session_id
FROM login_with_google