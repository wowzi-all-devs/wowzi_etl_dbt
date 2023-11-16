WITH companies AS
(SELECT  
  c.id,
  cp.name,
  cp.industry,
  cp.type,
  cp.twitterhandle,
  cp.twitterfollowers,
  cp.facebook_company_page,
  cp.linkedin_company_page,
  cp.website,
  cp.address,
  cp.phone,
  c.contacts,
  c.createdAt,
  cp.domain,
  cp.city,
  cp.state,
  cp.zip,
  cp.country,
  cp.about_us,
  c.archived,
  cp.is_public,
  cp.hs_lead_status,
  cp.hs_lastmodifieddate,
  cp.hs_date_exited_lead,
  cp.hs_time_in_lead,
  cp.total_money_raised,
  cp.hs_last_sales_activity_date,
  cp.recent_conversion_event_name,
  cp.hubspot_owner_id,
  cp.hs_user_ids_of_all_owners,
  cp.hubspot_owner_assigneddate,
  cp.hs_all_team_ids,
  cp.hubspot_team_id,
  cp.hs_all_owner_ids,
  cp.recent_deal_amount,
  cp.hs_date_entered_subscriber,
  cp.annualrevenue,
  cp.hs_created_by_user_id,
  cp.num_associated_deals,
  cp.hs_date_entered_customer,
  cp.hs_last_sales_activity_timestamp,
  cp.engagements_last_meeting_booked_campaign,
  cp.hubspotscore,
  cp.recent_deal_close_date,
  cp.hs_last_logged_call_date
  --cp.*
FROM {{ source('hubspot_staging', 'companies') }}  c
LEFT JOIN {{ source('hubspot_staging', 'companies_properties') }} cp ON c._airbyte_companies_hashid = cp._airbyte_companies_hashid
)

SELECT
  *
FROM companies