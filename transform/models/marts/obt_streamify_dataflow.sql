{{
  config(
    materialized = 'incremental',
    unique_key = 'event_id'
    )
}}

with 

events as (
    select * from {{ ref('fct_dataflow_events') }}
    {% if is_incremental() %}
        where measured_at > (select max(measured_at) from {{ this }})
    {% endif %}
),

dim_songs as (
    select * from {{ ref('dim_songs') }}
),

dim_users as (
    select *  from {{ ref('dim_users') }}
),

final as (
    select 

        events.* except(title, artist, album),
        dim_songs.* except (song_id),
        dim_users.* except (user_id),
    from events

    left join dim_songs
        on events.song_id = dim_songs.song_id
    left join dim_users 
        on events.user_id = dim_users.user_id
)

select * from final
