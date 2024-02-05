select 
date(a.event_datetime) event_date,
a.eventName eventname,
safe_cast(a.profile_id as string) profile_id,
'1' field,
safe_cast(b.influencer_id as string) influencer_id, 
from bi-staging-1-309112.wowzi_dbt_prod.clevertap_events a 
left join bi-staging-1-309112.wowzi_dbt_prod.influencer_facts b
on safe_cast(profile_id as string) = safe_cast(b.influencer_id as string)