with source as (
        select * from {{ source('raw_stream_data', 'raw_songs_dim') }}
  ),
  renamed as (
      select distinct
        {{ adapter.quote("title") }},
        {{ adapter.quote("artist") }},
        {{ adapter.quote("album") }},
        cast( replace( {{ adapter.quote("length") }}, ':', '.') as float64) as length,
        {{ adapter.quote("release_year") }},
        {{ adapter.quote("genre") }}

      from source
  ),

  final as (
    select
      {{dbt_utils.generate_surrogate_key(['title', 'artist', 'album'])}} as song_id,
      *
    from renamed
  )


  select * from final
    