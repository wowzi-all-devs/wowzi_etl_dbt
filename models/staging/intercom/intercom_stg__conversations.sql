
WITH
  conversations AS (
  SELECT
    id AS conversation_id,
    JSON_EXTRACT(tags, '$.tags[0].name') AS tag,
    JSON_EXTRACT_SCALAR(teammates, '$.admins[0].id') AS admin_id,
    JSON_EXTRACT_SCALAR(statistics, '$.time_to_admin_reply') AS time_to_admin_reply,
    JSON_EXTRACT_SCALAR(statistics, '$.count_conversation_parts') AS count_conversation_parts,
    JSON_EXTRACT_SCALAR(statistics, '$.median_time_to_reply') AS median_time_to_reply,
    JSON_EXTRACT_SCALAR(conversation_rating, '$.rating') AS conversation_rating,
    JSON_EXTRACT(conversation_rating, '$.remark') AS conversation_rating_remark,
    TIMESTAMP_SECONDS(created_at) AS created_at
  FROM
    {{ source('staging', 'conversations') }} )
SELECT
  conversations.*,
  admins.name AS admin_name,
  admins.email AS admin_email
FROM
  conversations
LEFT JOIN
  {{ source('staging', 'admins')}} admins
ON
  admins.id=conversations.admin_id