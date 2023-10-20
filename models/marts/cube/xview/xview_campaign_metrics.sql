{{ config(tags=["cube"]) }}

WITH
  base_x_view AS (
  SELECT
    cf.company_id,
    C.company_name,
    mmc.campaign_id,
    it.country,
    (
    SELECT
      country
    FROM
      {{ ref('postgres_stg__cluster_countries') }} clc
    WHERE
      clc.cluster_id = cc.cluster_id
      AND clc.country = it.country ) AS cluster_country,
    cf.currency,
    SUM(payment_amount_list) AS amount_spent,
    MAX( cf.budget ) AS campaign_budget,
    SUM(total_follower_count) AS potential_reach,
    SUM(total_engagement) AS total_engagement,
    SUM(total_influencers) AS total_influencers,
  FROM
    {{ ref('agg_manual_metric_campaign_channel') }} mmc
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
    third_verification_status = "APPROVED"

  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
  twitter_campaigns AS (
  SELECT
    cf.company_id,
    C.company_name,
    td.campaign_id,
    it.country,
    (
    SELECT
      country
    FROM
        {{ ref('postgres_stg__cluster_countries')}} clc
    WHERE
      clc.cluster_id = cc.cluster_id
      AND clc.country = it.country ) AS cluster_country,
    cf.currency,
    SUM(payment_amount_list) AS amount_spent,
    MAX( cf.budget ) AS campaign_budget,
    SUM(td.followers_count) AS potential_reach,
    SUM(reply_count+likes+retweet_count+quote_count) AS total_engagement,
    count(distinct td.influencer_id) AS total_influencers
  FROM
    {{ref('tweets_insights')}} td
  LEFT JOIN
    {{ ref('postgres_stg__campaigns') }} cf
    on td.campaign_id=cf.campaign_id
  LEFT JOIN
    {{ ref('influencer_task_facts') }} it
  ON
    it.task_id=td.task_id
  LEFT JOIN
    {{ ref('postgres_stg__companies') }} C on C.company_id=cf.company_id
  LEFT JOIN
    {{ref('postgres_stg__cluster_companies') }} cc
  ON
    cf.company_id = cc.company_id
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),

  intermediate_x_view as (
    select * from base_x_view union distinct select * from twitter_campaigns
  ),
  final_x_view AS (
  SELECT
    intermediate_x_view.*,
    ROUND((amount_spent / total_engagement), 2) AS cost_per_engagement
  FROM
    intermediate_x_view ),
dollar_x_view as(
SELECT
  final_x_view.*,
  ROUND( amount_spent / currency_rate, 2 ) AS dollar_amount_spent,
  ROUND( campaign_budget / currency_rate, 2 ) AS dollar_campaign_budget,
  ROUND( final_x_view.cost_per_engagement / currency_rate, 2 ) AS dollar_cost_per_engagement
FROM
  final_x_view
LEFT JOIN
  {{ ref('int_currency_rates') }} cr
ON
  cr.currency = final_x_view.currency)

select * from dollar_x_view