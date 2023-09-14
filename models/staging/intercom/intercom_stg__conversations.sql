
WITH
  conversations AS (
  SELECT
    cast(id as int) AS conversation_id,
    regexp_extract(REPLACE(JSON_EXTRACT(tags, '$.tags[0].name'), '"', ''), r'^[^:]*') as group_tag,
    JSON_EXTRACT(tags, '$.tags[0].name') AS tag,
    cast(JSON_EXTRACT_SCALAR(teammates, '$.admins[0].id') as int )AS admin_id,
    cast(JSON_EXTRACT_SCALAR(statistics, '$.time_to_admin_reply') as int )AS time_to_admin_reply,
    cast(JSON_EXTRACT_SCALAR(statistics, '$.count_conversation_parts')as int)AS count_conversation_parts,
    cast(JSON_EXTRACT_SCALAR(statistics, '$.median_time_to_reply')as int) AS median_time_to_reply,
    cast(JSON_EXTRACT_SCALAR(conversation_rating, '$.rating') as int) AS conversation_rating,
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
  cast(admins.id as int)=conversations.admin_id