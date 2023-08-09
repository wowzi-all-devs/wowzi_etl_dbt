{% snapshot creator_archived_tasks_snapshot %}

{{
    config(
      target_schema='wowzi_snapshots',
      unique_key='influencer_id',
      strategy='check',
      check_cols=['archived_tasks'],
    )
}}

SELECT  
    influencer_id,
    archived_tasks
FROM {{ ref('influencer_bad_behaviour_scoring') }}

{% endsnapshot %}