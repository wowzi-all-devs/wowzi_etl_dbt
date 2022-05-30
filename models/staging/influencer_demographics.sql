SELECT cast(id as INT64) as influencer_id,
    date(timestamp(creation_time)) as date_account_created,
    extract(year from date(timestamp(creation_time))) as year_account_created,
    extract(month from date(timestamp(creation_time))) as month_account_created,
    extract(day from date(timestamp(creation_time))) as day_account_created,
    first_name,
    last_name,
    email,
    role,
    mobile_number,
    gender,
    country,
    location,
    personal_id_number,
    income_currency,
    income_category,
    date(timestamp(dob)) dob,
    extract(
        year
        from date(timestamp(dob))
    ) as dob_year,
    extract(
        month
        from date(timestamp(dob))
    ) as dob_month,
    extract(
        day
        from date(timestamp(dob))
    ) as dob_day,
    payment_method,
    accepted_terms_version,
    platform,
    code,
    smileidentity_status,
    FROM {{ source('staging', 'influencers') }}
    -- FROM `bi-staging-1-309112.{}.{}`