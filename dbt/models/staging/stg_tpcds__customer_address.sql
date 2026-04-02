with source as (
    select * from {{ source('tpcds', 'customer_address') }}
),

renamed as (
    select * from source
)

select * from renamed
