{{ config(tags=["influencer_success"]) }}

WITH
  task_data AS(
  SELECT
    campaign_id,
    COUNT(task_id) AS total_tasks,
    COUNTIF(first_verification_status="APPROVED") quality_verified_tasks,
    COUNTIF(second_verification_status="APPROVED") problematic_verified_tasks,
    COUNTIF(third_verification_status="APPROVED") time_verified_tasks,
  FROM
    {{ ref('postgres_stg__influencer_tasks') }}
  GROUP BY
    1 
)
SELECT
  c.campaign_id,
  c.company_id,
  c.campaign_name,
  c.campaign_creation_date,
  c.start_date,
  c.end_date,
  c.currency,
  c.budget,
  ce.budget_spent,
  round(ce.budget_spent/ cr.currency_rate,2) as budget_spent_usd,
  c.bonus_amount,
  c.bonus_type,
  c.creator_type,
  c.creator_id,
  c.advertiser_skip_pre_approval,
  c.canceled,
  c.canceled_timestamp,
  ce.influencers_invited_count,
  ce.influencers_accepted_count,
  ce.company_name,
  task_data.total_tasks,
  task_data.quality_verified_tasks,
  task_data.problematic_verified_tasks,
  task_data.time_verified_tasks,
  adv.advertiser_id,
  adv.country
FROM
  {{ ref('postgres_stg__campaigns') }} c
LEFT JOIN
  task_data
ON
  c.campaign_id=task_data.campaign_id
LEFT JOIN
  {{ ref('campaign_expenditure') }} ce
ON
  c.campaign_id=CAST(ce.campaign_id AS integer)
LEFT JOIN
  {{ ref('postgres_stg__merchants') }} adv
ON
  c.merchant_id=adv.advertiser_id
LEFT JOIN
  {{ ref('int_currency_rates') }} cr
ON 
  DATE(c.end_date)=DATE(cr.date) AND c.currency=cr.currency
ORDER BY
  c.campaign_id DESC