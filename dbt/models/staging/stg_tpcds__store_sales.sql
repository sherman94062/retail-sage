with source as (
    select * from {{ source('tpcds', 'store_sales') }}
),

renamed as (
    select * from source
)

select * from renamed
