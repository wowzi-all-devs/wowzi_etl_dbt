with advertisers as (
    SELECT
        cast(id as INT64) as advertiser_id,
        cast(company_id as INT64) as company_id,
        date(creation_time) as date_account_created,
        first_name,
        last_name,
        email,
        if(role = "MERCHANT", "ADVERTISER", role) as role,
        phone,
        gender,
        country,
        location,
        city,
        personal_id_number,
        dob_date,
        avatar,
        -- FROM `bi-staging-1-309112.dims.merchants`
        FROM {{ source('core', 'merchants') }}
),
companies as (
    SELECT id as company_id,
        name,
        company_type,
        company_size,
        company_industry,
        kra_pin,
        address,
        postal_code,
        outgoing_currency,
        company_payment_method,
        company_payment_plan,
        date(creation_time) as company_creation_date,
        agency,
        agency_commission
    -- FROM `bi-staging-1-309112.dims.companies`
    FROM {{ ref('companies') }}
),
advertiser_job_acceptance as (
    select advertiser_id,
        sum(if (invitation_status = "ACCEPTED", 1, 0)) as total_accepted_jobs,
        sum(if (invitation_status = "EXPIRED", 1, 0)) as total_expired_jobs,
        sum(if (invitation_status = "NOT_ACCEPTED", 1, 0)) as total_not_accepted_jobs,
        from (
            SELECT j1.campaign_id,
                j1.invitation_status,
                j2.advertiser_id,
                --sum(if ( invitation_status = "ACCEPTED", 1, 0 ) ) as total_accepted,
                --sum(if ( invitation_status = "EXPIRED", 1, 0 ) ) as total_expired,
                --sum(if ( invitation_status = "NOT_ACCEPTED", 1, 0 ) ) as total_not_accepted,
            FROM {{ source('staging', 'jobs') }} as j1
                left join(
                    SELECT 
                        campaign_id,
                        merchant_id as advertiser_id,
                    FROM {{ ref('campaigns') }}
                ) as j2 on j1.campaign_id = j2.campaign_id
        )
    group by 1
),
last_campaign_date as (
    select 
        advertiser_id,
        campaign_creation_date as last_campaign_date
    from (
            SELECT 
                merchant_id as advertiser_id,
                campaign_creation_date,
                ROW_NUMBER() OVER(
                    PARTITION BY merchant_id
                    ORDER BY timestamp(start_date) desc
                ) as row_number,
            FROM {{ ref('campaigns') }}
            order by advertiser_id desc
        )
    where row_number = 1
),
first_campaign_date as (
    select 
        advertiser_id,
        campaign_creation_date as first_campaign_date
    from (
            SELECT 
                merchant_id as advertiser_id,
                campaign_creation_date,
                ROW_NUMBER() OVER(
                    PARTITION BY merchant_id
                    ORDER BY timestamp(start_date) asc
                ) as row_number,
                -- FROM dims.campaigns
            FROM {{ ref('campaigns') }}
            order by advertiser_id desc
        )
    where row_number = 1
),
budgets as (
    SELECT merchant_id as advertiser_id,
        count(*) as total_campaigns,
        sum(budget) as total_budget_allocation,
        sum(if(gender = "MALE", 1, 0)) as total_male_campaigns,
        sum(if(gender = "FEMALE", 1, 0)) as total_female_campaigns,
        sum(if(gender = "BOTH", 1, 0)) as total_mixed_campaigns,
        sum(bonus_amount) as total_bonus_amount
    -- FROM `bi-staging-1-309112.dims.campaigns`
    FROM {{ ref('campaigns') }}
    where pre_live_check_status = "APPROVED"
    group by 1
)
select *
from advertisers
    left join companies using(company_id)
    left join advertiser_job_acceptance using(advertiser_id)
    left join first_campaign_date using(advertiser_id)
    left join last_campaign_date using(advertiser_id)
    left join budgets using(advertiser_id)