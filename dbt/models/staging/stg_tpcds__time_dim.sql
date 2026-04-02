with source as (
    select * from {{ source('tpcds', 'time_dim') }}
),

renamed as (
    select * from source
)

select * from renamed
