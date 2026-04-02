with source as (
    select * from {{ source('tpcds', 'web_page') }}
),

renamed as (
    select * from source
)

select * from renamed
