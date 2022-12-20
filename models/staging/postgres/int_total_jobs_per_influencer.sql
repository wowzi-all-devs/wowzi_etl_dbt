SELECT influencer_id,
    count(distinct campaign_id) as offered_jobs,
    sum(if (invitation_status = "ACCEPTED", 1, 0)) as total_accepted,
    sum(if (invitation_status = "EXPIRED", 1, 0)) as total_expired,
    sum(if (invitation_status = "NOT_ACCEPTED", 1, 0)) as total_not_accepted,
    FROM {{ ref('postgres_stg__jobs') }}
group by 1
order by 2 desc