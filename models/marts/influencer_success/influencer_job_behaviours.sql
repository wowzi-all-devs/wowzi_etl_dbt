WITH influencer_tasks AS 
(SELECT  
  tf.influencer_id, 
  tf.campaign_id, 
  tf.job_id,
  tf.task_id,
  DATE(tf.task_creation_time) AS task_creation_date,
  tf.channel,
  tf.submission_link,
  tf.submission_status,
  c.end_date AS campaign_end_date,
  tf.country
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts` tf 
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.campaign_facts` c ON tf.campaign_id = c.campaign_id
  WHERE c.canceled IS NOT true),

job_tasks AS
(SELECT 
    i.influencer_id,
    i.job_id,
    count(DISTINCT i.task_id) AS no_of_tasks,
    min(i.task_creation_date) AS first_task_creation_date,
    i.campaign_end_date
FROM influencer_tasks i
    GROUP BY i.influencer_id, i.job_id, i.campaign_end_date),

jobs_submitted_tasks AS
(SELECT 
    i.influencer_id,
    i.job_id,
    count(DISTINCT task_id) AS no_of_tasks
FROM influencer_tasks i 
    WHERE i.submission_status = 'SUBMITTED'
    GROUP BY i.influencer_id, i.job_id),

approved_tasks AS
(SELECT 
    i.influencer_id,
    i.job_id,
    count(DISTINCT task_id) AS no_of_tasks
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts` i 
    WHERE coalesce(first_verification_status, second_verification_status) = 'APPROVED'
    GROUP BY i.influencer_id, i.job_id),

time_verified_tasks AS 
(SELECT 
    i.influencer_id,
    i.job_id,
    count(DISTINCT task_id) AS no_of_tasks
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts` i 
    WHERE third_verification_status = 'APPROVED'
    GROUP BY i.influencer_id, i.job_id),

archived_tasks AS
(SELECT 
    i.influencer_id,
    i.job_id,
    count(DISTINCT task_id) AS no_of_tasks
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts` i 
    WHERE third_verification_status = 'REJECTED'
    GROUP BY i.influencer_id, i.job_id)

SELECT 
    j1.influencer_id,
    j1.first_task_creation_date,
    j1.campaign_end_date,
    j1.job_id,
    j1.no_of_tasks,
    (CASE WHEN j2.no_of_tasks IS NULL THEN 0
    ELSE j2.no_of_tasks END) AS submitted_tasks,
    (j1.no_of_tasks - j2.no_of_tasks) AS tasks_not_submitted,
    (CASE WHEN (j1.no_of_tasks - j2.no_of_tasks) = 0 
    THEN 'COMPLETE'
    WHEN (j1.no_of_tasks - j2.no_of_tasks) > 0
    AND date(current_date) >= date(j1.campaign_end_date)
    THEN 'INCOMPLETE'
    WHEN j2.no_of_tasks IS NULL AND date(current_date) >= date(j1.campaign_end_date)
    THEN 'UNATTEMPTED'
    ELSE 'PENDING'
   END) AS job_status,
   a.no_of_tasks AS approved_tasks,
   t.no_of_tasks AS time_verified_tasks,
   ar.no_of_tasks as archived_tasks
FROM job_tasks j1
LEFT JOIN jobs_submitted_tasks j2 ON j1.influencer_id = j2.influencer_id
AND j1.job_id = j2.job_id
LEFT JOIN approved_tasks a ON j1.influencer_id = a.influencer_id
AND j1.job_id = a.job_id
LEFT JOIN time_verified_tasks t ON j1.influencer_id = t.influencer_id
AND j1.job_id = t.job_id
LEFT JOIN archived_tasks ar ON j1.influencer_id = ar.influencer_id
AND j1.job_id = ar.job_id