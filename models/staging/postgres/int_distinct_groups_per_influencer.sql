with influencer_groups as (
  SELECT 
      id as group_id,
      name as group_name,
      description as group_description,
  FROM {{ ref('postgres_stg__influencer_groups') }}
),
influencer_with_groups as (
  SELECT j1.*,
         influencer_groups.*
  FROM {{ ref('postgres_stg__influencer_group_influencers') }} as j1
  left join influencer_groups
  on j1.influencer_group_id = influencer_groups.group_id
)
SELECT
  influencer_id,
  count(distinct group_id) as distinct_groups
FROM influencer_with_groups
GROUP BY 1