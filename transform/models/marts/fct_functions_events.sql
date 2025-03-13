{{
  config(
    materialized = 'incremental',
    unique_key = 'event_id'
    )
}}

with source as (
    select * from {{ ref('stg_function_music_events') }}
    {% if is_incremental() %}
        where measured_at > (select max(measured_at) from {{ this }})
    {% endif %}
)

select * from source
