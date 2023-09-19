{{ config(
  tags = ["cube"]
) }}

WITH base_x_view AS (

  SELECT
    cf.company_id,
    C.company_name,
    mmc.campaign_id,
    it.country,
    (
      SELECT
        country
      FROM
        {{ ref('postgres_stg__cluster_countries')}} clc
      WHERE
        clc.cluster_id = cc.cluster_id
        AND clc.country = it.country
    ) AS cluster_country,
    cf.currency,
    SUM(payment_amount_list) AS amount_spent,
    MAX(
      cf.budget
    ) AS campaign_budget,
    SUM(total_follower_count) AS potential_reach,
    SUM(total_engagement) AS total_engagement,
    SUM(total_influencers) AS total_influencers,
  FROM
    {{ ref('agg_manual_metric_campaign_channel') }}
    mmc
    LEFT JOIN {{ ref('postgres_stg__campaigns') }}
    cf
    ON cf.campaign_id = mmc.campaign_id
    LEFT JOIN {{ ref('postgres_stg__companies') }} C
    ON C.company_id = cf.company_id
    LEFT JOIN {{ ref('influencer_task_facts') }}
    it
    ON it.campaign_id = cf.campaign_id
    LEFT JOIN {{ref('postgres_stg__cluster_companies')}} cc
    ON cf.company_id = cc.company_id
  WHERE
    third_verification_status = "APPROVED"
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6
),
final_x_view AS (
  SELECT
    base_x_view.*,
    ROUND((amount_spent / total_engagement), 2) AS cost_per_engagement
  FROM
    base_x_view
)
SELECT
  final_x_view.*,
  ROUND(
    amount_spent / currency_rate,
    2
  ) AS dollar_amount_spent,
  ROUND(
    campaign_budget / currency_rate,
    2
  ) AS dollar_campaign_budget,
  ROUND(
    final_x_view.cost_per_engagement / currency_rate,
    2
  ) AS dollar_cpe
FROM
  final_x_view
  LEFT JOIN {{ ref('int_currency_rates') }} cr
  ON cr.currency = final_x_view.currency
