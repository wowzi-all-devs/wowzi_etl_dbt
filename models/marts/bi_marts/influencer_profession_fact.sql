with a as
(
select * from 
--- {{ref('postgres_stg__influencer_subcategories')}} isc
`bi-staging-1-309112.wowzi_airbyte.influencer_subcategories` isc
left join 
-- {{ref('postgres_stg__subcategories')}} sc
`bi-staging-1-309112.wowzi_airbyte.subcategories` sc
on isc.subcategory_id = sc.id
where subcategory_type = 'OCCUPATION' 
),
inf as 
(
select 
a.influencer_id,
a.en_label profession,
b.influencer_id influencer_id_b
from a
right join bi-staging-1-309112.wowzi_dbt_prod.influencer_facts b
ON b.influencer_id = a.influencer_id
),
inf_data as
(
select
influencer_id_a,
inf_age_range,
gender,
initcap(location) location,
clean_country,
CASE 
    WHEN rank_facebook >= rank_instagram 
         AND rank_facebook >= rank_tiktok 
         AND rank_facebook >= rank_twitter THEN initcap(facebook_inf_level)
    WHEN rank_instagram >= rank_facebook 
         AND rank_instagram >= rank_tiktok 
         AND rank_instagram >= rank_twitter THEN initcap(instagram_inf_level)
    WHEN rank_tiktok >= rank_facebook 
         AND rank_tiktok >= rank_instagram 
         AND rank_tiktok >= rank_twitter THEN initcap(tiktok_inf_level)
    ELSE initcap(twitter_inf_level)
  END AS highest_category
FROM
(
  SELECT
    *,
    -- Assign numeric ranks to each category for comparison
    CASE facebook_inf_level 
      WHEN 'NONE' THEN 0
      WHEN 'NANO' THEN 1
      WHEN 'MICRO' THEN 2
      WHEN 'MACRO' THEN 3
      WHEN 'MEGA' THEN 4
      WHEN 'META' THEN 5
    END AS rank_facebook,
    
    CASE instagram_inf_level 
      WHEN 'NONE' THEN 0
      WHEN 'NANO' THEN 1
      WHEN 'MICRO' THEN 2
      WHEN 'MACRO' THEN 3
      WHEN 'MEGA' THEN 4
      WHEN 'META' THEN 5
    END AS rank_instagram,

    CASE tiktok_inf_level 
      WHEN 'NONE' THEN 0
      WHEN 'NANO' THEN 1
      WHEN 'MICRO' THEN 2
      WHEN 'MACRO' THEN 3
      WHEN 'MEGA' THEN 4
      WHEN 'META' THEN 5
    END AS rank_tiktok,

    CASE twitter_inf_level 
      WHEN 'NONE' THEN 0
      WHEN 'NANO' THEN 1
      WHEN 'MICRO' THEN 2
      WHEN 'MACRO' THEN 3
      WHEN 'MEGA' THEN 4
      WHEN 'META' THEN 5
    END AS rank_twitter
  
  FROM {{ ref('influencer_reg_job_facts_cob') }}
  --`bi-staging-1-309112.wowzi_dbt_prod.influencer_reg_job_facts_cob` 
)
where clean_country is not null
and  location is not null
and inf_age_range is not null
and gender is not null
and inf_profession is not null
and lower(email) not like '%@getnada.com%'
AND lower(email) not like '%wowzi%'
AND lower(email) not like '%@fivermail.com%'
AND lower(email) not like '%@g.com%'
AND lower(email) not like '%@example.com%'
AND lower(email) not like '%@getairmail.com%'
)
select 
inf.influencer_id_b influencer_id,
case when inf.profession is null then 'n/a' else inf.profession end as profession,
case when (inf_age_range is null) or (inf_age_range = 'N0 DOB') then 'n/a' else inf_age_range end as inf_age_range,
case when clean_country is null then 'n/a' else clean_country end as country,
case when highest_category is null then 'n/a' else highest_category end as level,
case when gender is null then 'n/a' else gender end as gender
from 
inf 
LEFT JOIN
inf_data
on SAFE_CAST(inf.influencer_id_b as INT64) = SAFE_CAST(inf_data.influencer_id_a as INT64)