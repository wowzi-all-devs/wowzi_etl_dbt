{{ config(tags=["business_development"]) }}

WITH advertisers_grouped as (
  SELECT
    merchant_id,
    currency,
    extract(month from date(timestamp(campaign_start_date))) as campaign_start_month,
    SUM(campaign_budget) as total_spent
  FROM 
    {{ ref('campaign_expenditure') }}
  WHERE
    canceled=False
  GROUP BY
    1, 2, 3
),
advertisers_with_name as (
  SELECT
    {{ dbt_utils.surrogate_key(['merchant_id', 'campaign_start_month', 'currency']) }} as primary_key,
    advertisers_grouped.merchant_id,
    adv.name as company_name,
    adv.country as country,
    advertisers_grouped.campaign_start_month,
    advertisers_grouped.total_spent,
    advertisers_grouped.currency
  FROM
    advertisers_grouped
  LEFT JOIN
    {{ ref('dims_advertisers') }} adv
  ON 
    advertisers_grouped.merchant_id=adv.advertiser_id
)
SELECT *
FROM 
  advertisers_with_name
ORDER BY
  total_spent desc