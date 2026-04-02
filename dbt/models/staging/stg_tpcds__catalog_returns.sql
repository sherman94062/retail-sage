with source as (
    select * from {{ source('tpcds', 'catalog_returns') }}
),

renamed as (
    select * from source
)

select * from renamed
