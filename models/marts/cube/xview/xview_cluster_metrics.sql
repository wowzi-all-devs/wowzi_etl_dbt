{{ config(tags=["cube"]) }}

SELECT
  cluster_id,
  cluster_name,
  COUNT(DISTINCT campaign_id) AS total_campaigns,
  SUM(campaign_budget) AS total_campaign_budget,
  SUM(amount_spent) AS total_spend,
  SUM(total_influencers) AS total_influencers,
  SUM(total_engagement) AS total_engagements,
  SUM(potential_reach) AS total_potential_reach,
  ROUND(AVG(cost_per_engagement),0) AS avg_cpe
FROM
  {{ ref('xview_campaign_metrics')}} xm
LEFT JOIN
  {{ ref('postgres_stg__cluster_companies')}} cc
ON
  cc.company_id=xm.company_id
left join {{ ref('postgres_stg__clusters')}} cl using(cluster_id)
GROUP BY
  1,2