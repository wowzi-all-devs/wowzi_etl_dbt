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
inf.influencer_group_id 
from {{ source('staging', 'influencer_group_influencers') }} inf
right join i_groups ------Right join bc my right tbl is where all the groups are that i need matched to inf_id
on inf.influencer_group_id = i_groups.id 
),
inf_data as
(
select
inf.group_id,
inf.group_name,
inf.influencer_id, 
concat(inf_data.first_name,' ', inf_data.last_name) inf_name,
inf_data.gender,
inf_data.inf_age_range,
inf_data.job_eligibility,
inf_data.job_activity,
inf_data.clean_country,
initcap(inf_data.location) location,
inf_data.completed_one_job
from inf 
LEFT JOIN 
{{ ref('influencer_reg_job_facts_cob') }} inf_data
ON cast(inf.influencer_id as Int64) = cast(inf_data.influencer_id_a as Int64)
)
SELECT * FROM inf_data