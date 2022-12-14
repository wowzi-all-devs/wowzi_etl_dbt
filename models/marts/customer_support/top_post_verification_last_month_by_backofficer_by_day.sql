{{ config(tags=["customer_support"]) }}


WITH approvals AS (
  SELECT task_id,
    date(creation_time) AS date_creation,
    submission_status,
    top_post,
    date(top_post_selected_time) AS top_post_selected_time,
    submission_link,
    cast(first_verification_feedback_backofficer_id AS int) AS backofficer_id,
  FROM {{ref('postgres_stg__influencer_tasks') }}
  WHERE top_post IS TRUE
),
bo AS (
  SELECT
    backofficer_id,
    backofficer_name,
    email,
    role
  FROM {{ ref('postgres_stg__back_officers') }}
),
joined_tables AS (
  SELECT *
  FROM approvals
  LEFT JOIN bo USING(backofficer_id)
)
SELECT
  {{ dbt_utils.surrogate_key(['top_post_selected_time', 'backofficer_id']) }} as primary_key, 
  top_post_selected_time,
  backofficer_id,
  backofficer_name,
  count(top_post) AS num_top_posts_verification
FROM joined_tables
WHERE 
  backofficer_id IS NOT NULL
  AND date_diff(date(current_timestamp()),date(top_post_selected_time), MONTH) <=1
  AND backofficer_id IN (select backofficer_id from bo)
GROUP BY 2,3,4
ORDER BY 2 DESC