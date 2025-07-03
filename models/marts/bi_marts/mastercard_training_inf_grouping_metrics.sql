with i_groups as
(
select id,name
from 
{{ source('staging', 'influencer_groups') }} 
-- `bi-staging-1-309112`.`wowzi_airbyte`.`influencer_groups` 
where id in (2742,2743,2764,2784,2779,2780, 2783,2782, 2781,2778,2741,2776,2810)
),
inf as
(
select 
i_groups.id group_id, 
i_groups.name group_name, 
inf.influencer_id, 
inf.influencer_group_id,
date(inf.creation_time) creation_time,
from {{ source('staging', 'influencer_group_influencers') }} inf
-- from `bi-staging-1-309112`.`wowzi_airbyte`.`influencer_group_influencers` inf
right join i_groups ------Right join bc my right tbl is where all the groups are that i need matched to inf_id
on inf.influencer_group_id = i_groups.id 
),
jb as
(
select 
influencer_id,
task_id,
sum(tasks_assigned) tasks_assigned,
sum(amount_lcy) job_value_lcy,
sum(completed_tasks) completed_tasks
from {{ref('influencer_job_breakdown')}}
-- from `bi-staging-1-309112`.`wowzi_dbt_prod`.`influencer_job_breakdown`
where invitation_status = 'Accepted'
and job_status <> 'Failed'
group by influencer_id, task_id
),
payment as
(
select 
influencer_id,
payment_id,
task_id,
currency,
exchange_rate,
sum(paid_amount) paid_amount,
sum(paid_amount_usd) paid_amount_usd,
fine_payment_status
from {{ref('creator_payments_information')}}
--  bi-staging-1-309112.wowzi_dbt_prod.creator_payments_information
 group by influencer_id, payment_id, currency, fine_payment_status, exchange_rate, task_id
)
-- select * from payment where task_id = 337734
,
inf_data as
(
select
inf.group_id,
inf.group_name,
inf.influencer_id, 
inf.creation_time,
concat(inf_data.first_name,' ', inf_data.last_name) inf_name,
inf_data.gender,
inf_data.inf_age_range,
inf_data.job_eligibility,
initcap(inf_data.smileidentity_status) smileidentity_status,
inf_data.job_activity,
inf_data.clean_country,
initcap(inf_data.location) location,
inf_data.completed_one_job,
Initcap(inf_data.facebook_status) facebook_status,
initcap(inf_data.instagram_status) instagram_status,
initcap(inf_data.tiktok_status) tiktok_status,
initcap(inf_data.twitter_status) twitter_status,
jb.task_id,
jb.tasks_assigned,
case when 
  jb.completed_tasks is null then 0 
  else jb.completed_tasks
  end as completed_tasks,
jb.job_value_lcy,
from inf 
LEFT JOIN 
-- {{ ref('influencer_reg_job_facts_cob') }} inf_data
`bi-staging-1-309112`.`wowzi_dbt_prod`.`influencer_reg_job_facts_cob` inf_data
ON cast(inf.influencer_id as Int64) = cast(inf_data.influencer_id_a as Int64)
LEFT JOIN jb
ON inf.influencer_id = jb.influencer_id
where inf.influencer_id is not null
),
final as
(
select 
inf_data.*,
case when
  payment.fine_payment_status in ('Successful', 'Manual') then payment.paid_amount
  else 0
  end as paid_amt,
case when
  payment.fine_payment_status in ('Successful', 'Manual') then payment.paid_amount_usd
  else 0
  end as paid_amt_usd,
 CASE 
    WHEN SUM(paid_amount) OVER (PARTITION BY inf_data.influencer_id) > 13000 THEN 'Above 13k'
    ELSE 'Below 13k'
  END AS payout_bucket

from inf_data 
LEFT JOIN payment 
on cast(inf_data.task_id as Int64) = cast(payment.task_id as Int64)
group by 
group_id,
group_name,
influencer_id, 
creation_time,
inf_name,
gender,
inf_age_range,
job_eligibility,
smileidentity_status,
job_activity,
clean_country,
location,
task_id,
completed_one_job,
facebook_status,
instagram_status,
tiktok_status,
twitter_status,
tasks_assigned,
completed_tasks,
job_value_lcy,
fine_payment_status,
paid_amount,
paid_amount_usd
)
SELECT 
*
 FROM 
final
