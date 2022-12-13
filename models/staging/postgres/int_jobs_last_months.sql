with jobs_previous_3_months as (
    SELECT 
        cast(influencer_id as INT64) AS influencer_id,
        count(id) as jobs_last_3_months,
        FROM {{ ref('postgres_stg__jobs') }}
    where invitation_status = "ACCEPTED"
        and date_diff(date(current_timestamp()),date(offer_creation_time),MONTH) <= 3
    group by 1
    order by 2 desc
),
jobs_previous_1_month as (
    SELECT 
        cast(influencer_id as INT64) AS influencer_id,
        count(id) as jobs_last_month,
        FROM {{ ref('postgres_stg__jobs') }}
    where invitation_status = "ACCEPTED"
        and date_diff(date(current_timestamp()),date(offer_creation_time),MONTH) <= 1
    group by 1
    order by 2 desc
),
jobs_previous_6_months as (
    SELECT 
        cast(influencer_id as INT64) AS influencer_id,
        count(id) as jobs_last_6_months,
        FROM {{ ref('postgres_stg__jobs') }}
    where invitation_status = "ACCEPTED"
        and date_diff(date(current_timestamp()),date(offer_creation_time),MONTH) <= 6
    group by 1
    order by 2 desc
),
jobs_previous_2_months as (
    SELECT 
        cast(influencer_id as INT64) AS influencer_id,
        count(id) as jobs_last_2_months,
        FROM {{ ref('postgres_stg__jobs') }}
    where invitation_status = "ACCEPTED"
        and date_diff(date(current_timestamp()),date(offer_creation_time),MONTH) <= 2
    group by 1
    order by 2 desc
),
jobs_previous_2_weeks as (
    SELECT 
        cast(influencer_id as INT64) AS influencer_id,
        count(id) as jobs_last_2_weeks,
        FROM {{ ref('postgres_stg__jobs') }}
    where invitation_status = "ACCEPTED"
        and date_diff(date(current_timestamp()),date(offer_creation_time),WEEK) <= 2
    group by 1
    order by 2 desc
),
jobs_previous_4_weeks as (
    SELECT 
        cast(influencer_id as INT64) AS influencer_id,
        count(id) as jobs_last_4_weeks,
        FROM {{ ref('postgres_stg__jobs') }}
    where invitation_status = "ACCEPTED"
        and date_diff(date(current_timestamp()),date(offer_creation_time),WEEK) <= 4
    group by 1
    order by 2 desc
),
jobs_previous_1_week as (
    SELECT
        cast(influencer_id as INT64) AS influencer_id,
        count(id) as jobs_last_week,
    FROM {{ ref('postgres_stg__jobs') }}
    where invitation_status = "ACCEPTED"
        and date_diff(date(current_timestamp()), date(offer_creation_time), WEEK) <= 1
    group by 1
    order by 2 desc
),
inf as (
    SELECT cast(id as INT64) as influencer_id,
        FROM {{ ref('postgres_stg__influencers') }}
)
select *
from inf
    left join jobs_previous_6_months using(influencer_id)
    left join jobs_previous_3_months using(influencer_id)
    left join jobs_previous_2_months using(influencer_id)
    left join jobs_previous_1_month using(influencer_id)
    left join jobs_previous_4_weeks using(influencer_id)
    left join jobs_previous_2_weeks using(influencer_id)
    left join jobs_previous_1_week using(influencer_id)