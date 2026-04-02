with source as (
    select * from {{ source('tpcds', 'customer') }}
),

renamed as (
    select * from source
)

select * from renamed
