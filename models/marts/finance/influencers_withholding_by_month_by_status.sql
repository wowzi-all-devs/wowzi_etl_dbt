{{ config(tags=["finance"]) }}

WITH payouts AS (
  SELECT 
    influencer_id,
    FORMAT_DATE("%Y-%m-01", creation_time) AS month,
    status as payment_status,
    SUM(amount) as total_amount
  FROM 
    {{ ref('influencer_transfers') }}
  GROUP BY 1, 2, 3
  HAVING SUM(amount) >=24000
)
SELECT
  {{ dbt_utils.surrogate_key(['payouts.influencer_id', 'payouts.month', 'payouts.payment_status']) }} as primary_key,
  payouts.*,
  CONCAT(first_name, " ", last_name) AS name,
  email,
  mobile_number
FROM 
  {{ ref('influencers') }}
RIGHT JOIN 
  payouts USING (influencer_id)
WHERE 
  country="KE"
ORDER BY
  month desc