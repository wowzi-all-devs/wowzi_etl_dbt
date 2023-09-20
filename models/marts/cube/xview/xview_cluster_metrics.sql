{{ config(
  tags = ["cube"]
) }}

SELECT
  cluster_id,
  cluster_name,
  cluster_country,
  COUNT(
    DISTINCT campaign_id
  ) AS total_campaigns,
  SUM(campaign_budget) AS total_budget,
  SUM(amount_spent) AS amount_spent,
  SUM(total_influencers) AS total_influencers,
  SUM(total_engagement) AS total_engagement,
  SUM(potential_reach) AS potential_reach,
  ROUND(AVG(cost_per_engagement), 0) AS cost_per_engagement,
  sum(dollar_amount_spent) as dollar_amount_spent,
  sum(dollar_campaign_budget) as dollar_total_budget,
  ROUND(AVG(dollar_cost_per_engagement),0) as dollar_cost_per_engagement

FROM
  {{ ref('xview_campaign_metrics') }}
  xm
  LEFT JOIN {{ ref('postgres_stg__cluster_companies') }}
  cc
  ON cc.company_id = xm.company_id
  LEFT JOIN {{ ref('postgres_stg__clusters') }}
  cl USING(cluster_id)
WHERE
  cluster_country IS NOT NULL
GROUP BY
  1,
  2,3
