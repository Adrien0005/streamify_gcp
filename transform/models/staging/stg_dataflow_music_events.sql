with source as (
        select * from {{ source('raw_stream_data', 'dataflow_music_events') }}
  ),
  renamed as (
      select
      
        {{ adapter.quote("event_id") }},
        {{ adapter.quote("event_type") }},
        {{ adapter.quote("user_id") }} as user_email,
        {{ adapter.quote("title") }},
        {{ adapter.quote("artist") }},
        {{ adapter.quote("album") }},
        {{ adapter.quote("coordinates") }},
        cast ({{ adapter.quote("datetime") }} as datetime ) as measured_at,
        {{ adapter.quote("ingested_at") }}

      from source
  ),

  final as (
      select 
          *,
        {{dbt_utils.generate_surrogate_key(['title', 'artist', 'album'])}} as song_id,
        {{dbt_utils.generate_surrogate_key(['user_email', 'coordinates.latitude', 'coordinates.longitude'])}} as user_id,
        datetime_diff(ingested_at, measured_at, MILLISECOND) as ingestion_delay_ms
      from renamed
  )

  select * from final 
    