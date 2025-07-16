select 
*,
case when
  lower(smileidentity_status) = 'approved' 
  and job_eligibility and 
  (instagram_linked = 'linked' 
  or facebook_linked = 'linked')
  and bank_account_number is not null
  then influencer_id
  else null end as 
  passed_onboarding_criteria
from
(
select 
a.influencer_id,
b.country,
a.date_account_created,
initcap(a.smileidentity_status) smileidentity_status,
a.job_eligibility,
b.instagram_linked,
b.facebook_linked,
c.bank_account_number,
concat('Q',extract(quarter from a.date_account_created)) quarter,
concat('Q',extract(quarter from a.date_account_created), '-', (extract(year from a.date_account_created))) qtr_yr

 from  
  bi-staging-1-309112.wowzi_dbt_prod.influencer_facts  a
left join
 bi-staging-1-309112.wowzi_dbt_prod.social_media_link_table b
on 
  a.influencer_id = b.influencer_id
left join
  bi-staging-1-309112.wowzi_dbt_prod.bank_details c
on 
  a.influencer_id = c.influencer_id
-- where safe_cast(a.date_account_created as DATE) >= '2025-01-01'
)
