with source as (
    select * from {{ source('tpcds', 'catalog_page') }}
),

renamed as (
    select * from source
)

select * from renamed
