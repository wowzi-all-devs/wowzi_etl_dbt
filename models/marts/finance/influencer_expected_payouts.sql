{{ config(tags=["finance"]) }}



WITH
payments_due AS (
  SELECT
    task.influencer_id,
    first_name || ' ' || last_name as influencer_name,
    first_verification_status,
    second_verification_status,
    third_verification_status,
    mobile_number,
    task.campaign_id,
    comp.company_name,
    task.task_id,
    datetime(first_verification_feedback_time,"Africa/Nairobi") first_verification_datetime,
    datetime(transfers.payment_eligible_at) as actual_payment_date,
    date_diff(date(payment_eligible_at),date(third_verification_feedback_time), day) as days_since_third_verfication,
    payment_amount_list,
    influencers.country,
    bank_details.bank_name,
    bank_account_number,
    transfers.status,
    cmp.currency AS currency

    FROM {{ ref('postgres_stg__influencer_tasks') }} task
    left join {{ ref('postgres_stg__influencer_transfers') }} transfers on task.task_id=transfers.task_id
    LEFT JOIN {{ ref('postgres_stg__influencers') }} influencers on task.influencer_id=influencers.influencer_id
    LEFT JOIN {{ ref('int_bank_details') }} bank_details on task.influencer_id=bank_details.influencer_id
    LEFT JOIN {{ ref('postgres_stg__campaigns') }} cmp ON task.campaign_id=cmp.campaign_id
  LEFT JOIN {{ ref('postgres_stg__companies') }} comp on cmp.company_id=comp.company_id

  WHERE
    third_verification_status="APPROVED" and lower(transfers.status) in ('waiting_for_payment','failed')
    and canceled_timestamp is null
  ORDER BY
    first_verification_feedback_time DESC
),
weekdays AS (
  SELECT
      distinct*,
      FORMAT_DATE('%a',payments_due.actual_payment_date) AS actual_payment_date_weekday_name,
  FROM payments_due
),
latest_currency as (
  select date, cr.currency, currency_rate from  {{ ref('int_currency_rates') }} cr inner join (select currency, max(date) as MaxDate from {{ ref('int_currency_rates') }} group by 1)tm on tm.currency=cr.currency and tm.MaxDate=cr.date

)
SELECT
  weekdays.*,
  CASE
    WHEN actual_payment_date_weekday_name IN('Mon','Tue','Wed','Fri', 'Sat', 'Sun') THEN DATE_ADD(DATE_TRUNC(actual_payment_date, WEEK(THURSDAY)), INTERVAL 1 WEEK)
    WHEN actual_payment_date_weekday_name IN('Thu') AND EXTRACT(hour FROM actual_payment_date)>=17 THEN DATE_ADD(DATE_TRUNC(actual_payment_date, WEEK(THURSDAY)), INTERVAL 1 WEEK)
  END
  AS expected_payment_date,
  round(payment_amount_list/lc.currency_rate,2) as payment_amount_in_dollars

FROM weekdays left join latest_currency lc on lc.currency=weekdays.currency order by expected_payment_date desc



