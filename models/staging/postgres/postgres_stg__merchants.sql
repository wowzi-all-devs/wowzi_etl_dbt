{{ config(materialized='view') }}

with source as (

    select * from {{ source('staging', 'merchants') }}

),

renamed as (

    select
        _airbyte_unique_key,
        id as advertiser_id,
        city,
        code,
        role,
        email,
        phone,
        avatar,
        gender,
        country,
        dob_date,
        location,
        password,
        last_name,
        company_id,
        created_at,
        first_name,
        updated_at,
        email_token,
        code_attempts,
        date(creation_time) as date_account_created,
        code_timestamp,
        manually_premium,
        personal_id_number,
        email_token_attempts,
        email_token_timestamp,
        accepted_terms_version,
        phone_verification_status,
        email_notifications_payment_done,
        email_notifications_campaign_outcome,
        email_notifications_campaign_started,
        email_notifications_influencers_matched

    from source

)

select * from renamed
