with twitter as (
    select influencer_id,
        true as twitter,
    FROM {{ ref('postgres_stg__influencer_channel_data') }}
    where channel = 'TWITTER'
),
fb as (
    select influencer_id,
        true as facebook,
    FROM {{ ref('postgres_stg__influencer_channel_data') }}
    where channel = 'FACEBOOK'
),
ig as (
    select influencer_id,
        true as instagram,
    FROM {{ ref('postgres_stg__influencer_channel_data') }}
    where channel = 'INSTAGRAM'
),
influencer_taxes_info as (
    select influencer_id, tax_external_id
    FROM {{ ref('postgres_stg__influencer_taxes_info') }}
)
select *
from (
        select j5.*,
            j6.jobs_last_6_months as jobs_last_6_months,
            j6.jobs_last_3_months as jobs_last_3_months,
            j6.jobs_last_2_months as jobs_last_2_months,
            j6.jobs_last_month as jobs_last_month,
            j6.jobs_last_4_weeks as jobs_last_4_weeks,
            j6.jobs_last_2_weeks as jobs_last_2_weeks,
            j6.jobs_last_week as jobs_last_week,
            from (
                select j3.*,
                    j4.offered_jobs as offered_jobs,
                    j4.total_accepted as total_accepted,
                    j4.total_expired as total_expired,
                    j4.total_not_accepted as total_not_accepted,
                    round(j4.total_accepted / j4.offered_jobs, 2) as job_accept_rate,
                    round(j4.total_expired / j4.offered_jobs, 2) as job_expiry_rate,
                    round(j4.total_not_accepted / j4.offered_jobs, 2) as job_reject_rate,
                    from (
                        select j1.*,
                            j2.distinct_groups as distinct_groups,
                        from (
                            select 
                                id5.*,
                                id6.*
                                except (influencer_id)
                            from (
                                    select 
                                        id3.*,
                                        id4.*
                                        except (influencer_id),
                                    from (
                                            SELECT 
                                                id.*,
                                                id2.*
                                                except(influencer_id),
                                            from {{ref('int_influencer_demographics')}} as id
                                            left join (
                                                select *
                                                -- from `bi-staging-1-309112.temp_tables.approval_rates`
                                                from {{ref('int_post_approval_rates')}}
                                            ) as id2 on id.influencer_id = id2.influencer_id
                                        ) as id3
                                    left join(
                                        select *
                                        from {{ref('int_influencer_first_campaign')}}
                                    ) as id4 on id3.influencer_id = id4.influencer_id
                                ) as id5
                            left join(
                                select *
                                -- from `bi-staging-1-309112.temp_tables.last_campaign`
                                from {{ref('int_influencer_last_campaign')}}
                            ) as id6 on id5.influencer_id = id6.influencer_id
                        ) as j1
                        left join(
                            SELECT *
                            -- FROM `bi-staging-1-309112.temp_tables.num_distinct_groups`
                            from {{ref('int_distinct_groups_per_influencer')}}
                        ) as j2 on j1.influencer_id = j2.influencer_id
                    ) as j3
                    left join (
                        -- SELECT influencer_id,
                        --     count(distinct campaign_id) as offered_jobs,
                        --     sum(if (invitation_status = "ACCEPTED", 1, 0)) as total_accepted,
                        --     sum(if (invitation_status = "EXPIRED", 1, 0)) as total_expired,
                        --     sum(if (invitation_status = "NOT_ACCEPTED", 1, 0)) as total_not_accepted,
                        --     FROM `bi-staging-1-309112.dims.jobs`
                        -- group by 1
                        -- order by 2 desc
                        SELECT *
                        from {{ref('int_total_jobs_per_influencer')}}
                    ) as j4 on j3.influencer_id = j4.influencer_id
            ) as j5
            left join(
                select *
                -- from `bi-staging-1-309112.temp_tables.jobs_last_3_months`
                from {{ref('int_jobs_last_months')}}
            ) as j6 on cast(j5.influencer_id as INT64) = cast(j6.influencer_id as INT64)
    )
    left join fb using(influencer_id)
    left join ig using(influencer_id)
    left join twitter using(influencer_id)
    left join influencer_taxes_info using(influencer_id)