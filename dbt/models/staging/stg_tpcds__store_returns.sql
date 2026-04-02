with source as (
    select * from {{ source('tpcds', 'store_returns') }}
),

renamed as (
    select * from source
)

select * from renamed
