WITH job_breakdown as
(SELECT 
    * 
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown` ),

influencer_occupations as 
(
SELECT
  distinct
  safe_cast(isb.influencer_id as int) influencer_id,
  s.parent_category_id,
  c.enum_value as parent_category,
  isb.subcategory_id,
  s.enum_value
FROM `bi-staging-1-309112.wowzi_airbyte.influencer_subcategories` isb
left join `bi-staging-1-309112.wowzi_airbyte.subcategories`s on isb.subcategory_id = s.id
left join `bi-staging-1-309112.wowzi_airbyte.categories` c on s.parent_category_id = c.id
where c.enum_value = 'PROFESSION'
and isb.subcategory_type = 'OCCUPATION'
order by isb.influencer_id 
)

select a.*, b.parent_category_id,
initcap(b.parent_category) parent_category,
b.subcategory_id,
initcap(b.enum_value) enum_value
from job_breakdown a 
left join influencer_occupations b 
on a.influencer_id = b.influencer_id