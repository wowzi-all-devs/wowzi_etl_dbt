WITH deals AS 
(SELECT  
  d.id,
  d.dealname,
  d.companies,
  d.deal___select_brand_name,
  d.properties_payment_net brand_payment_segment,
  d.dealtype,
  d.hs_priority,
  d.dealstage,
  d.pipeline,
  d.deal_currency_code,
  d.hs_forecast_amount,
  d.hs_closed_amount,
  d.deal___country,
  d.num_notes,
  d.notes_last_contacted,
  d.notes_next_activity_date,
  d.hs_is_open_count,
  d.days_to_close,
  d.hs_is_closed,
  d.closedate,
  d.hs_is_closed_won,
  d.hs_closed_won_date,
  d.hs_closed_won_count,
  d.hs_time_in_closedwon,
  d.hs_date_entered_closedlost,
  d.closed_lost_reason,
  d.deal_loss_reason,
  d.createdAt,
  d.hs_created_by_user_id,
  d.updatedAt,
  d.hs_updated_by_user_id,
  d.archived,
  d.contacts,
  d.hubspot_owner_id,
  d.hubspot_owner_assigneddate
FROM {{ ref('hubspot_stg_deals') }} d 

)

SELECT 
    *
FROM deals
