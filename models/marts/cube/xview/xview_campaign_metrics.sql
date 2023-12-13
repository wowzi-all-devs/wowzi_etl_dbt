{{ config(tags=["cube"]) }}

WITH
  metrics_by_campaign_by_channel AS (
    select
      campaign_id,
      channel,
      count(distinct influencer_id) as total_influencers,
      sum(views) as total_views,
      sum(shares) as total_shares,
      sum(likes) as total_likes,
      sum(comments) as total_comments,
      (sum(views)+sum(shares)+sum(likes)+sum(comments)) as total_engagement,
      sum(influencer_followers) as influencer_followers,
      max(updated_at) as updated_at

    from {{ ref('campaign_post_platform_metrics') }}
    group by 1, 2
  ),

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
    cc.cluster_id AS cluster_id,
    cf.currency,
    cf.start_date as campaign_date,
    SUM(payment_amount_list) AS amount_spent,
    MAX( cf.budget ) AS campaign_budget,
    SUM(influencer_followers) AS potential_reach,
    SUM(total_engagement) AS total_engagement,
    SUM(total_influencers) AS total_influencers,
  FROM
    metrics_by_campaign_by_channel mmc
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
    6,
    7,
    8),

  final_x_view AS (
  SELECT
    base_x_view.*,
    if(total_engagement!=0, ROUND((amount_spent / total_engagement), 2), amount_spent) AS cost_per_engagement
  FROM
    base_x_view ),
dollar_x_view as(
SELECT
  final_x_view.*,
  (CASE WHEN cr.currency_rate IS NULL THEN ROUND( amount_spent / cr1.currency_rate, 2 )
  ELSE ROUND( amount_spent / cr.currency_rate, 2 ) END) AS dollar_amount_spent,
  (CASE WHEN cr.currency_rate IS NULL THEN ROUND( campaign_budget / cr1.currency_rate, 2 )
  ELSE ROUND( campaign_budget / cr.currency_rate, 2 ) END) AS dollar_campaign_budget,
  (CASE WHEN cr.currency_rate IS NULL THEN ROUND( final_x_view.cost_per_engagement / cr1.currency_rate, 2 )
  ELSE ROUND( final_x_view.cost_per_engagement / cr.currency_rate, 2 ) END) AS dollar_cost_per_engagement
FROM
  final_x_view
LEFT JOIN
  {{ ref('int_currency_rates') }} cr
ON
  cr.currency = final_x_view.currency
  and date(cr.date) = date(final_x_view.campaign_date)
LEFT JOIN
  {{ ref('int_currency_rates') }} cr1
ON
  cr1.currency = final_x_view.currency
  and date(cr1.date) = date_add(date(final_x_view.campaign_date), interval -1 day))

select * from dollar_x_view
