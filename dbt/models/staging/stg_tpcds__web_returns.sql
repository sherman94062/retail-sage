with source as (
    select * from {{ source('tpcds', 'web_returns') }}
),

renamed as (
    select * from source
)

select * from renamed
