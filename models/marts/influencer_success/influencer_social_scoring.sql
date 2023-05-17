WITH influencer_stats AS 
(SELECT  
  influencer_id,
  (CASE WHEN total_campaigns IS NULL THEN 0
  ELSE total_campaigns END) AS total_campaigns,
  (CASE WHEN offered_jobs IS NULL THEN 0
  ELSE offered_jobs END) AS offered_jobs,
  (CASE WHEN total_accepted IS NULL THEN 0
  ELSE total_accepted END) AS total_accepted,
  (CASE WHEN total_expired IS NULL THEN 0
  ELSE total_expired END) AS total_expired,
  (CASE WHEN total_not_accepted IS NULL THEN 0
  ELSE total_not_accepted END) AS total_rejected,
  last_campaign_date,
  (CASE WHEN date_diff(current_date,DATE(last_campaign_date), day) IS NULL THEN 0
  ELSE date_diff(current_date,DATE(last_campaign_date), day) END) AS campaign_recency_days
FROM {{ ref('influencer_facts') }}
  WHERE job_eligibility IS true),

total_tasks AS 
(SELECT 
  tf.influencer_id, 
  (CASE WHEN count(DISTINCT tf.task_id) IS NULL THEN 0 
  ELSE count(DISTINCT tf.task_id) END) AS total_tasks
FROM {{ ref('campaign_facts') }}  cf 
LEFT JOIN {{ ref('influencer_task_facts') }} tf ON cf.campaign_id = tf.campaign_id
  WHERE canceled IS NULL
  GROUP by tf.influencer_id),

total_tasks_failed_first_verification AS
(SELECT 
  tf.influencer_id, 
  (CASE WHEN count(DISTINCT tf.task_id) IS NULL THEN 0
  ELSE count(DISTINCT tf.task_id) END) AS total_tasks_failed_first_verification
FROM {{ ref('campaign_facts') }} cf 
LEFT JOIN {{ ref('influencer_task_facts') }} tf ON cf.campaign_id = tf.campaign_id
AND tf.first_verification_status IN ('PROBLEMATIC')
  WHERE canceled IS NULL
  GROUP BY tf.influencer_id ),

total_tasks_failed_third_verification AS
(SELECT 
  tf.influencer_id, 
  (CASE WHEN count(DISTINCT tf.task_id) IS NULL THEN 0
  ELSE count(DISTINCT tf.task_id) END) AS total_tasks_failed_third_verification
FROM {{ ref('campaign_facts') }} cf 
LEFT JOIN {{ ref('influencer_task_facts') }} tf ON cf.campaign_id = tf.campaign_id
AND tf.third_verification_status IN ('REJECTED')
  WHERE canceled IS NULL
  GROUP BY tf.influencer_id),

total_top_posts AS
(SELECT 
  tf.influencer_id, 
  (CASE WHEN count(DISTINCT tf.task_id) IS NULL THEN 0
  ELSE count(DISTINCT tf.task_id) END) AS top_posts
FROM {{ ref('campaign_facts') }} cf 
LEFT JOIN {{ ref('influencer_task_facts') }} tf ON cf.campaign_id = tf.campaign_id
AND tf.top_post IS true
  WHERE canceled IS NULL
  GROUP BY tf.influencer_id),

all_creator_metrics AS
(SELECT
i.influencer_id,
i.total_campaigns,
i.offered_jobs,
i.total_accepted,
(CASE WHEN total_accepted = 0 THEN 0 
WHEN total_accepted IS NULL THEN 0 
ELSE (total_accepted/offered_jobs)*10 END) AS job_acceptance_ratio,
i.total_expired,
(CASE WHEN total_expired = 0 THEN 0 
WHEN total_expired IS NULL THEN 0 
ELSE (total_expired/offered_jobs)*10 END) AS job_expiry_ratio,
(CASE WHEN i.total_rejected IS NULL THEN 0
ELSE i.total_rejected END) AS total_rejected,
i.last_campaign_date,
i.campaign_recency_days,
(CASE WHEN tt.total_tasks IS NULL THEN 0 
ELSE tt.total_tasks END) AS total_tasks,
(CASE WHEN ttf.total_tasks_failed_first_verification IS NULL THEN 0
ELSE ttf.total_tasks_failed_first_verification END) AS total_tasks_failed_first_verification,
(CASE WHEN ttf.total_tasks_failed_first_verification = 0 THEN 0
WHEN ttf.total_tasks_failed_first_verification IS NULL THEN 0
ELSE (ttf.total_tasks_failed_first_verification/tt.total_tasks)*10 END) as task_approval_ratio,
(CASE WHEN ttf2.total_tasks_failed_third_verification IS NULL THEN 0
ELSE ttf2.total_tasks_failed_third_verification END) AS total_tasks_failed_third_verification,
(CASE WHEN ttf2.total_tasks_failed_third_verification = 0 THEN 0
WHEN ttf2.total_tasks_failed_third_verification IS NULL THEN 0
ELSE (ttf2.total_tasks_failed_third_verification/tt.total_tasks)*10 END) as task_archiving_ratio,
(CASE WHEN tp.top_posts IS NULL THEN 0 
ELSE tp.top_posts END) AS top_posts,
(CASE WHEN tp.top_posts = 0 THEN 0
WHEN tp.top_posts IS NULL THEN 0
ELSE (tp.top_posts/tt.total_tasks)*10 END) AS top_posts_ratio
FROM influencer_stats i 
LEFT JOIN total_tasks tt ON i.influencer_id = tt.influencer_id
LEFT JOIN total_tasks_failed_first_verification ttf ON i.influencer_id = ttf.influencer_id
LEFT JOIN total_tasks_failed_third_verification ttf2 ON i.influencer_id = ttf2.influencer_id
LEFT JOIN total_top_posts tp ON i.influencer_id = tp.influencer_id
  WHERE i.total_campaigns > 0),

max_counters AS 
(SELECT 
  max(total_campaigns) as max_no_of_campaigns,
  max(total_accepted) as max_no_of_accepted_jobs,
  max(total_expired) as max_no_of_expired_jobs,
  max(campaign_recency_days) as max_campaign_recency_days,
  max(total_tasks) as max_total_tasks,
  max(total_tasks_failed_first_verification) as max_total_tasks_failed_first_verification,
  max(total_tasks_failed_third_verification) as max_total_tasks_failed_third_verification,
  max(top_posts) as max_top_posts
FROM all_creator_metrics),

final_scores AS
(SELECT 
  a.influencer_id,
  a.total_campaigns,
  a.offered_jobs,
  a.total_accepted,
  a.job_acceptance_ratio,
  a.total_expired,
  a.job_expiry_ratio,
  a.total_rejected,
  a.last_campaign_date,
  a.campaign_recency_days,
  a.total_tasks,
  a.total_tasks_failed_first_verification,
  a.task_approval_ratio,
  a.total_tasks_failed_third_verification,
  a.task_archiving_ratio,
  a.top_posts,
  a.top_posts_ratio,
  (10.0/m.max_no_of_campaigns)*a.total_campaigns AS total_campaigns_score,
  a.job_acceptance_ratio AS job_acceptance_ratio_score,
  (10.0/m.max_no_of_accepted_jobs)*a.total_accepted AS total_accepted_jobs_score,
  10 - ((10.0/m.max_no_of_expired_jobs)*a.total_expired) AS total_expired_jobs_score,
  10 - a.job_expiry_ratio AS job_expiry_ratio_score,
  10 - ((10.0/m.max_campaign_recency_days)*a.campaign_recency_days) AS campaign_recency_score,
  (10.0/m.max_total_tasks)*a.total_tasks AS total_tasks_score,
  a.task_approval_ratio AS task_approval_ratio_score,
  10 - ((10.0/m.max_total_tasks_failed_first_verification)*a.total_tasks_failed_first_verification) AS total_tasks_failed_first_verification_score,
  10 - ((10.0/m.max_total_tasks_failed_third_verification)*a.total_tasks_failed_third_verification) AS total_tasks_failed_third_verification_score,
  10 - a.task_archiving_ratio AS task_archiving_ratio_score,
  (10.0/m.max_top_posts)*a.top_posts AS top_posts_score,
  a.top_posts_ratio AS top_posts_ratio_score
FROM all_creator_metrics a
LEFT JOIN max_counters m ON m.max_no_of_campaigns = m.max_no_of_campaigns),

wowzi_ranks AS
(SELECT 
  f.influencer_id,
  f.total_campaigns,
  f.offered_jobs,
  f.total_accepted,
  f.job_acceptance_ratio,
  f.total_expired,
  f.job_expiry_ratio,
  f.total_rejected,
  f.last_campaign_date,
  f.campaign_recency_days,
  f.total_tasks,
  f.total_tasks_failed_first_verification,
  f.task_approval_ratio,
  f.total_tasks_failed_third_verification,
  f.task_archiving_ratio,
  f.top_posts,
  f.top_posts_ratio,
  f.total_campaigns_score,
  f.job_acceptance_ratio_score,
  f.total_accepted_jobs_score,
  f.total_expired_jobs_score,
  f.job_expiry_ratio_score,
  f.campaign_recency_score,
  f.total_tasks_score,
  f.task_approval_ratio_score,
  f.total_tasks_failed_first_verification_score,
  f.total_tasks_failed_third_verification_score,
  f.task_archiving_ratio_score,
  f.top_posts_score,
  f.top_posts_ratio_score,
  ((0.2*f.total_campaigns_score) +
  (0.1*f.total_tasks_score) +
  (0.15*f.job_acceptance_ratio_score) +
  (0.15*f.total_accepted_jobs_score) +
  (0.1*f.campaign_recency_score) +
  (0.1*f.task_approval_ratio_score) +
  (0.08*f.total_tasks_failed_first_verification_score) +
  (0.03*f.task_archiving_ratio_score) +
  (0.02*f.total_tasks_failed_third_verification_score) +
  (0.05*f.top_posts_ratio_score) +
  (0.02*f.top_posts_score)) as wowzi_rank
FROM final_scores f),

wowzi_segments AS
(SELECT 
  w.influencer_id,
  w.total_campaigns,
  w.offered_jobs,
  w.total_accepted,
  w.job_acceptance_ratio,
  w.total_expired,
  w.job_expiry_ratio,
  w.total_rejected,
  w.last_campaign_date,
  w.campaign_recency_days,
  w.total_tasks,
  w.total_tasks_failed_first_verification,
  w.task_approval_ratio,
  w.total_tasks_failed_third_verification,
  w.task_archiving_ratio,
  w.top_posts,
  w.top_posts_ratio,
  w.total_campaigns_score,
  w.job_acceptance_ratio_score,
  w.total_accepted_jobs_score,
  w.total_expired_jobs_score,
  w.job_expiry_ratio_score,
  w.campaign_recency_score,
  w.total_tasks_score,
  w.task_approval_ratio_score,
  w.total_tasks_failed_first_verification_score,
  w.total_tasks_failed_third_verification_score,
  w.task_archiving_ratio_score,
  w.top_posts_score,
  w.top_posts_ratio_score,
  w.wowzi_rank,
  (CASE WHEN w.wowzi_rank = 10 THEN 'Ultimate Super Creator'
  WHEN w.wowzi_rank >= 7 AND w.wowzi_rank < 10 THEN 'Super Creator'
  WHEN w.wowzi_rank >= 5 AND w.wowzi_rank < 7 THEN 'Proficient'
  WHEN w.wowzi_rank >= 4.5 AND w.wowzi_rank < 5 THEN 'Emergiing'
  WHEN w.wowzi_rank >= 4 AND w.wowzi_rank < 4.5 THEN 'Junior'
  WHEN w.wowzi_rank >= 3 AND w.wowzi_rank < 4 THEN 'Apprentice II'
  WHEN w.wowzi_rank >= 2.5 AND w.wowzi_rank < 3 THEN 'Apprentice I'
  WHEN w.wowzi_rank >= 2 AND w.wowzi_rank < 2.5 THEN 'Rookie'
  WHEN w.wowzi_rank >= 1 AND w.wowzi_rank < 2 THEN 'Novice'
  END) AS creator_segment
FROM wowzi_ranks w)

SELECT 
  i.influencer_id,
  i.total_campaigns,
  i.offered_jobs,
  i.total_accepted,
  ws.job_acceptance_ratio,
  ws.total_expired,
  ws.job_expiry_ratio,
  ws.total_rejected,
  ws.last_campaign_date,
  ws.campaign_recency_days,
  ws.total_tasks,
  ws.total_tasks_failed_first_verification,
  ws.task_approval_ratio,
  ws.total_tasks_failed_third_verification,
  ws.task_archiving_ratio,
  ws.top_posts,
  ws.top_posts_ratio,
  ws.total_campaigns_score,
  ws.job_acceptance_ratio_score,
  ws.total_accepted_jobs_score,
  ws.total_expired_jobs_score,
  ws.job_expiry_ratio_score,
  ws.campaign_recency_score,
  ws.total_tasks_score,
  ws.task_approval_ratio_score,
  ws.total_tasks_failed_first_verification_score,
  ws.total_tasks_failed_third_verification_score,
  ws.task_archiving_ratio_score,
  ws.top_posts_score,
  ws.top_posts_ratio_score,
  ws.wowzi_rank,
  (CASE WHEN i.offered_jobs = 0 THEN 'Un-Engaged Neophyte'
  WHEN i.offered_jobs > 0 AND i.total_campaigns = 0 THEN 'No Jobs Accepted'
  ELSE ws.creator_segment END) AS creator_segment
FROM influencer_stats i 
LEFT JOIN wowzi_segments ws ON i.influencer_id = ws.influencer_id