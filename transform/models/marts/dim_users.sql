with source as (
    select * from {{ ref('stg_raw_users_dim') }}
)


select * from source
