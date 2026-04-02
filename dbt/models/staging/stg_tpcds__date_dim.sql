with source as (
    select * from {{ source('tpcds', 'date_dim') }}
),

renamed as (
    select * from source
)

select * from renamed
