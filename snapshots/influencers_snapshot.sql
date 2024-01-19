{% snapshot influencers_snapshot %}

{{
    config(
      target_schema='wowzi_snapshots',
      unique_key='influencer_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

SELECT
*
FROM {{ ref('postgres_stg__influencers') }}

{% endsnapshot %}