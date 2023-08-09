{% snapshot creator_incomplete_jobs_snapshot %}

{{
    config(
      target_schema='wowzi_snapshots',
      unique_key='influencer_id',
      strategy='check',
      check_cols=['no_of_incomplete_jobs'],
    )
}}

SELECT  
    influencer_id,
    no_of_incomplete_jobs
FROM {{ ref('influencer_bad_behaviour_scoring') }}

{% endsnapshot %}