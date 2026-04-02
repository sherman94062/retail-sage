with source as (
    select * from {{ source('tpcds', 'web_sales') }}
),

renamed as (
    select * from source
)

select * from renamed
