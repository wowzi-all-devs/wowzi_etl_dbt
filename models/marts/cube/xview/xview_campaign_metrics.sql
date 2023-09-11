{{ config(tags=["cube"]) }}

WITH
  base_x_view AS (
  SELECT
    cf.company_id,
    c.company_name,
    mmc.campaign_id,
    it.country,
    sum(payment_amount_list) as amount_spent,
    MAX(cf.budget) AS campaign_budget,
    SUM(total_follower_count) AS potential_reach,
    SUM(total_engagement) AS total_engagement,
    SUM(total_influencers) AS total_influencers,
  FROM
    {{ref('agg_manual_metric_campaign_channel')}} mmc
  left  JOIN
    {{ ref('postgres_stg__campaigns') }}cf
  ON
    cf.campaign_id=mmc.campaign_id

   left join {{ref('postgres_stg__companies')}} c on c.company_id=cf.company_id
   left join {{ ref('influencer_task_facts')}} it on it.campaign_id=cf.campaign_id
   where third_verification_status="APPROVED"  
  GROUP BY
    1,
    2,3,4),
  final_x_view AS (
  SELECT
    base_x_view.*,
    ROUND((amount_spent/total_engagement),2) AS cost_per_engagement
  FROM
    base_x_view)

  select * from final_x_view