with i_groups as
(
select id,name
from 
{{ source('staging', 'influencer_groups') }} 
where id in (2742,2743,2764,2784,2779,2780, 2783,2782, 2781,2778,2741,2776)
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
right join i_groups ------Right join bc my right tbl is where all the groups are that i need matched to inf_id
on inf.influencer_group_id = i_groups.id 
),
jb as
(
select 
influencer_id,
sum(amount_lcy) job_value_lcy,
sum(completed_tasks) completed_tasks
from {{ref('influencer_job_breakdown')}}
where invitation_status = 'Accepted'
and job_status <> 'Failed'
group by influencer_id
),
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
jb.completed_tasks,
jb.job_value_lcy
from inf 
LEFT JOIN 
{{ ref('influencer_reg_job_facts_cob') }} inf_data
ON cast(inf.influencer_id as Int64) = cast(inf_data.influencer_id_a as Int64)
LEFT JOIN jb
ON inf.influencer_id = jb.influencer_id
where inf.influencer_id is not null
)
SELECT * FROM inf_data