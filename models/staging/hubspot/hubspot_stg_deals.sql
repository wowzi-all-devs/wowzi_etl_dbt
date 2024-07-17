WITH deals AS
(SELECT  
  d.id,
  dp.dealname,
  dp.dealtype,
  dp.hs_forecast_amount,
  dp.hs_closed_amount,
  dp.deal_currency_code,
  dp.hs_priority,
  dp.dealstage,
  dp.hs_deal_stage_probability,
  dp.deal___country,
  dp.hs_num_of_associated_line_items,
  dp.hs_analytics_latest_source_timestamp,
  dp.num_notes,
  dp.num_contacted_notes,
  dp.engagements_last_meeting_booked,
  dp.hs_sales_email_last_replied,
  dp.notes_last_contacted,
  dp.pipeline,
  dp.notes_next_activity_date,
  dp.hs_is_open_count,
  dp.days_to_close,
  dp.hs_is_closed,
  dp.closedate,
  dp.hs_is_closed_won,
  dp.hs_closed_won_date,
  dp.hs_closed_won_count,
  dp.hs_time_in_closedwon,
  dp.hs_date_entered_closedlost,
  dp.closed_lost_reason,
  dp.deal_loss_reason,
  d.createdAt,
  dp.hs_created_by_user_id,
  d.updatedAt,
  dp.hs_updated_by_user_id,
  d.archived,
  d.companies,
  dp.deal___select_brand_name,
  d.contacts,
  dp.hubspot_owner_id,
  dp.hubspot_owner_assigneddate,
  dp.hs_all_owner_ids,
  dp.hs_all_team_ids,
  dp.hubspot_team_id
FROM {{ source('hubspot_staging', 'deals') }} d
LEFT JOIN {{ source('hubspot_staging', 'deals_properties') }} dp ON d._airbyte_raw_id = dp._airbyte_deals_hashid
)

SELECT 
  *
FROM deals