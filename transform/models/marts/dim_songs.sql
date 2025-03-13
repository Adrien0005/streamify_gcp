with source as (
    select * from {{ ref('stg_raw_songs_dim') }}
)


select * from source
