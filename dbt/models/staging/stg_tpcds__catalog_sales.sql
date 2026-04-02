with source as (
    select * from {{ source('tpcds', 'catalog_sales') }}
),

renamed as (
    select * from source
)

select * from renamed
