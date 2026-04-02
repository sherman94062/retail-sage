with source as (
    select * from {{ source('tpcds', 'web_site') }}
),

renamed as (
    select * from source
)

select * from renamed
