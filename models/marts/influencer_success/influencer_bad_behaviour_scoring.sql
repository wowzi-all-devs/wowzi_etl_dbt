WITH total_job_tasks AS 
(SELECT  
  influencer_id,
  job_id,
  cf.end_date AS campaign_end_date,
  (CASE WHEN count(DISTINCT task_id) IS NULL THEN 0
  ELSE count(DISTINCT task_id) END) AS no_of_tasks
FROM {{ ref('influencer_task_facts') }}  tf 
LEFT JOIN {{ ref('campaign_facts') }} cf ON tf.campaign_id = cf.campaign_id
  GROUP BY influencer_id, job_id, cf.end_date),

submitted_tasks AS
(SELECT  
  influencer_id,
  job_id,
  (CASE WHEN count(DISTINCT task_id) IS NULL THEN 0
  ELSE count(DISTINCT task_id) END) AS submitted_tasks
FROM {{ ref('influencer_task_facts') }} 
  WHERE submission_status = 'SUBMITTED'
  GROUP BY influencer_id, job_id),

job_status AS
(SELECT 
    t.influencer_id,
    t.job_id,
    t.campaign_end_date,
    t.no_of_tasks,
    (CASE WHEN s.submitted_tasks IS NULL THEN 0
    ELSE s.submitted_tasks END) AS submitted_tasks,
    (CASE WHEN (t.no_of_tasks - s.submitted_tasks) IS NULL THEN  t.no_of_tasks
    ELSE (t.no_of_tasks - s.submitted_tasks) END) as non_submitted_tasks,
    (CASE WHEN t.no_of_tasks - s.submitted_tasks = 0 THEN 'COMPLETE'
    WHEN t.no_of_tasks - s.submitted_tasks > 0 AND current_date >= t.campaign_end_date
    THEN 'INCOMPLETE'
    WHEN s.submitted_tasks IS NULL AND current_date >= t.campaign_end_date
    THEN 'INCOMPLETE'
    ELSE 'PENDING' END) as job_status
FROM total_job_tasks t 
LEFT JOIN submitted_tasks s ON t.influencer_id = s.influencer_id
AND t.job_id = s.job_id),

job_ratios AS 
(SELECT 
    j1.influencer_id,
    count(DISTINCT j1.job_id) AS no_of_jobs,
    count(DISTINCT j2.job_id) AS no_of_incomplete_jobs,
    (CASE WHEN count(DISTINCT j2.job_id) = 0 THEN 0
    ELSE (count(DISTINCT j2.job_id)/count(DISTINCT j1.job_id))*10 END) AS jobs_incomplete_ratio
FROM job_status j1
LEFT JOIN job_status j2 on j1.influencer_id = j2.influencer_id
AND j2.job_status = 'INCOMPLETE'
    GROUP BY j1.influencer_id),

task_ratios AS 
(SELECT 
    j1.influencer_id,
    sum(j1.no_of_tasks) AS no_of_tasks,
    (CASE WHEN sum(j2.no_of_tasks) IS NULL THEN 0
    ELSE sum(j2.no_of_tasks) END) AS no_of_incomplete_tasks,
    (CASE WHEN sum(j2.no_of_tasks) = 0 THEN 0
    WHEN sum(j2.no_of_tasks) IS NULL THEN 0
    ELSE (sum(j2.no_of_tasks)/sum(j1.no_of_tasks))*10 END) AS tasks_incomplete_ratio
FROM job_status j1
LEFT JOIN job_status j2 on j1.influencer_id = j2.influencer_id
AND j1.job_id = j2.job_id AND j2.job_status = 'INCOMPLETE'
    GROUP BY j1.influencer_id),

approved_tasks AS 
(SELECT  
  influencer_id,
  count(distinct task_id) as no_of_tasks
FROM {{ ref('influencer_task_facts') }} 
  where coalesce(first_verification_status,second_verification_status) = 'APPROVED'
  group by influencer_id),

archived_tasks AS 
(SELECT  
  influencer_id,
  count(distinct task_id) as no_of_tasks
FROM {{ ref('influencer_task_facts') }} 
  where third_verification_status = 'REJECTED'
  and coalesce(first_verification_status, second_verification_status) = 'APPROVED'
  group by influencer_id),

archived_ratios AS
(SELECt 
    DISTINCT
    jt.influencer_id,
    (CASE WHEN ap.no_of_tasks IS NULL THEN 0 
    ELSE ap.no_of_tasks END) AS approved_tasks,
    (CASE WHEN art.no_of_tasks IS NULL THEN 0
    ELSE art.no_of_tasks END) AS archived_tasks,
    (CASE WHEN art.no_of_tasks = 0 THEN 0 
    WHEN art.no_of_tasks IS NULL THEN 0
    WHEN ap.no_of_tasks IS NULL THEN 0
    ELSE (art.no_of_tasks/ap.no_of_tasks)*10 END) as task_archiving_ratio
FROM total_job_tasks jt 
LEFT JOIN approved_tasks ap ON jt.influencer_id = ap.influencer_id
LEFT JOIN archived_tasks art ON jt.influencer_id = art.influencer_id),

max_no_of_incomplete_jobs AS 
(SELECT
  max(no_of_incomplete_jobs) AS max_no_of_incomplete_jobs
FROM job_ratios),

max_no_of_incomplete_tasks AS 
(SELECT 
  max(no_of_incomplete_tasks) AS max_no_of_incomplete_tasks
FROM task_ratios),

max_no_archived_tasks AS
(SELECT 
  max(archived_tasks) AS max_no_archived_tasks
FROM archived_ratios),

first_level_scores AS
(SELECT 
  jr.influencer_id,
  jr.no_of_jobs,
  jr.no_of_incomplete_jobs,
  jr.jobs_incomplete_ratio,
  tr.no_of_tasks,
  tr.no_of_incomplete_tasks,
  tr.tasks_incomplete_ratio,
  ar.approved_tasks,
  ar.archived_tasks,
  ar.task_archiving_ratio,
  (10.0/mj.max_no_of_incomplete_jobs)*jr.no_of_incomplete_jobs AS no_of_incomplete_jobs_score,
  jr.jobs_incomplete_ratio AS jobs_incomplete_ratio_score,
  (10.0/mt.max_no_of_incomplete_tasks)*tr.no_of_incomplete_tasks AS no_of_incomplete_tasks_score,
  tr.tasks_incomplete_ratio AS tasks_incomplete_ratio_score,
  (10.0/mat.max_no_archived_tasks)*ar.archived_tasks AS archived_tasks_score,
  ar.task_archiving_ratio AS task_archiving_ratio_score,
  (0.2*((10.0/mj.max_no_of_incomplete_jobs)*jr.no_of_incomplete_jobs) +
  0.3*(jr.jobs_incomplete_ratio) +
  0.15*((10.0/mt.max_no_of_incomplete_tasks)*tr.no_of_incomplete_tasks) +
  0.15*(tr.tasks_incomplete_ratio) +
  0.1*((10.0/mat.max_no_archived_tasks)*ar.archived_tasks) +
  0.1*(ar.task_archiving_ratio)) as bad_behaviour_score
FROM job_ratios jr 
LEFT JOIN task_ratios tr ON jr.influencer_id = tr.influencer_id
LEFT JOIN archived_ratios ar ON jr.influencer_id = ar.influencer_id
LEFT JOIN max_no_of_incomplete_jobs mj ON mj.max_no_of_incomplete_jobs = mj.max_no_of_incomplete_jobs
LEFT JOIN max_no_of_incomplete_tasks mt on mt.max_no_of_incomplete_tasks = mt.max_no_of_incomplete_tasks
LEFT JOIN max_no_archived_tasks mat on mat.max_no_archived_tasks = mat.max_no_archived_tasks)

SELECT 
  f.influencer_id,
  f.no_of_jobs,
  f.no_of_incomplete_jobs,
  f.jobs_incomplete_ratio,
  f.no_of_tasks,
  f.no_of_incomplete_tasks,
  f.tasks_incomplete_ratio,
  f.approved_tasks,
  f.archived_tasks,
  f.task_archiving_ratio,
  f.jobs_incomplete_ratio_score,
  f.tasks_incomplete_ratio_score,
  f.archived_tasks_score,
  f.task_archiving_ratio_score,
  f.bad_behaviour_score,--,
  (CASE WHEN f.bad_behaviour_score >= 5 THEN 'MISDEMEANOR MASTERS'
  WHEN f.bad_behaviour_score >= 4 AND f.bad_behaviour_score <= 5 
  THEN 'UPCOMING MISDEMEANOR MASTERS'
  WHEN f.bad_behaviour_score >= 2 AND f.bad_behaviour_score <= 3 
  THEN 'MISBEHAVIOUR MAVERICKS'
  WHEN f.bad_behaviour_score >= 1 
  THEN 'INFRACTION INITIATES'
  WHEN f.bad_behaviour_score = 0 THEN 'BEST BEHAVED'
  END) as bad_behaviour_segment
FROM first_level_scores f