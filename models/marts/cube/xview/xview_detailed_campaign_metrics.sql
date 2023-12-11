{{ config(tags=["cube"]) }}

WITH
  campaign_metrics AS (
SELECT 
  campaign_id,
  channel,
  influencer_id,
  updated_at
FROM {{ ref('campaign_post_platform_metrics') }} ),

final_view AS
(SELECT 
    cf.company_id,
    C.company_name,
    mmc.campaign_id,
    mmc.influencer_id,
    it.country,
    (
    SELECT
      country
    FROM
      {{ ref('postgres_stg__cluster_countries') }} clc
    WHERE
      clc.cluster_id = cc.cluster_id
      AND clc.country = it.country ) AS cluster_country
FROM campaign_metrics mmc
LEFT JOIN
{{ ref('postgres_stg__campaigns') }} cf
ON
cf.campaign_id = mmc.campaign_id
LEFT JOIN
{{ ref('postgres_stg__companies') }} C
ON
C.company_id = cf.company_id
LEFT JOIN
{{ ref('influencer_task_facts') }} it
ON
it.campaign_id = cf.campaign_id
LEFT JOIN
{{ ref('postgres_stg__cluster_companies') }} cc
ON
cf.company_id = cc.company_id
WHERE
third_verification_status = "APPROVED")

SELECT 
    *
FROM final_view