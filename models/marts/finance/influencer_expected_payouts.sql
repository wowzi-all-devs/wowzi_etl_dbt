{{ config(tags=["finance"]) }}

WITH
payments_due AS (
  SELECT
    influencer_id,
    first_verification_status,
    second_verification_status,
    mobile_number,
    task.campaign_id,
    task.id AS task_id,
    datetime(first_verification_feedback_time,"Africa/Nairobi") first_verification_datetime,
    DATE_ADD(datetime(first_verification_feedback_time,"Africa/Nairobi"), INTERVAL 120 hour) payment_date,
    payment_amount_list,
    country,
    bank_name,
    bank_account_number,
    cmp.currency AS currency
  FROM {{ ref('influencer_tasks') }} task
  LEFT JOIN {{ ref('influencers') }} USING (influencer_id)
  LEFT JOIN {{ ref('bank_details') }} USING (influencer_id)
  LEFT JOIN {{ ref('campaigns') }} cmp ON task.campaign_id=cmp.id
  WHERE
    first_verification_status="APPROVED"
    AND DATE(first_verification_feedback_time, 'Africa/Nairobi') > DATE_SUB(CURRENT_DATE('Africa/Nairobi'), INTERVAL 2 week)
  ORDER BY
    first_verification_feedback_time DESC
),
weekdays AS (
  SELECT
      *,
      FORMAT_DATE('%a',payments_due.payment_date) AS weekday_name_abbreviated,
  FROM payments_due
)
SELECT
*, CASE
    WHEN LOWER(payment_status)  not in ('successful','manual') and date(payment_dates) < current_date() THEN "Late"
    WHEN payment_status is null then "Pending Payment"
    WHEN lower(payment_status) in ('successful','manual') then 'Payment Done'
END as payment_fulfillment
FROM(SELECT
  {{ dbt_utils.surrogate_key(['weekdays.influencer_id', 'weekdays.campaign_id', 'weekdays.task_id']) }} as primary_key,
  weekdays.*,
  CASE
    WHEN weekday_name_abbreviated IN('Fri', 'Sat', 'Sun') THEN DATE_ADD(DATE_TRUNC(payment_date, WEEK(MONDAY)), INTERVAL 1 WEEK)
    WHEN weekday_name_abbreviated IN('Tue','Wed') THEN DATE_ADD(DATE_TRUNC(payment_date, WEEK(ThURSDAY)), INTERVAL 1 WEEK)
    WHEN weekday_name_abbreviated IN('Thu') AND EXTRACT(hour FROM payment_date)<17 THEN DATE_TRUNC(payment_date, WEEK(THURSDAY))
    WHEN weekday_name_abbreviated IN('Thu') AND EXTRACT(hour FROM payment_date)>=17 THEN DATE_ADD(DATE_TRUNC(payment_date, WEEK(MONDAY)), INTERVAL 1 WEEK)
    WHEN weekday_name_abbreviated IN('Mon') AND EXTRACT(hour FROM payment_date)<17 THEN DATE_TRUNC(payment_date, WEEK(MONDAY))
    WHEN weekday_name_abbreviated IN('Mon') AND EXTRACT(hour FROM payment_date)>=17 THEN DATE_ADD(DATE_TRUNC(payment_date, WEEK(THURSDAY)), INTERVAL 1 WEEK)
  END
  AS payment_dates,
  CASE 
    WHEN lower(weekdays.bank_name)='mpesa' or lower(weekdays.bank_name)='airtel kenya' THEN 'Mobile Money' ELSE 'Bank' 
  END
  AS payment_method,
  it.status as payment_status
FROM weekdays
LEFT JOIN {{ ref('influencer_transfers') }} it using(task_id))
