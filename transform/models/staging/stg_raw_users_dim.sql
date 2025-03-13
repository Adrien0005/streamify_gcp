with source as (
        select * from {{ source('raw_stream_data', 'raw_users_dim') }}
  ),
  renamed as (
      select distinct
        {{ adapter.quote("gender") }},
        {{ adapter.quote("name") }},
        {{ adapter.quote("location") }},
        {{ adapter.quote("email") }},
        {{ adapter.quote("login") }},
        {{ adapter.quote("dob") }},
        {{ adapter.quote("registered") }},
        {{ adapter.quote("phone") }},
        {{ adapter.quote("cell") }},
        {{ adapter.quote("id") }},
        {{ adapter.quote("picture") }},
        {{ adapter.quote("nat") }}

      from source
  ),

  final as (
    select
      {{dbt_utils.generate_surrogate_key(['email', 'location.coordinates.latitude', 'location.coordinates.longitude'])}} as user_id,
      *
    from renamed
  )

  select * from final
    