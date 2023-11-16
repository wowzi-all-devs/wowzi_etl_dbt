WITH app_installs AS 
(SELECT 
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
FROM {{ ref('clevertap_stg_events') }} 
  where eventName = 'App Installed'),

logins AS 
(SELECT 
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
  login_session_id,
  row_number() over(partition by date(event_datetime),eventName, profile_id order by profile_id,eventName, event_datetime) as row_num
FROM {{ ref('clevertap_stg_events') }}
  where eventName in ('LoginMethod','LoginWithApple', 'LoginWithFacebook', 'LoginWithGoogle')
  and profile_id is not null)

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
FROM app_installs
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
FROM logins
    WHERE row_num = 1