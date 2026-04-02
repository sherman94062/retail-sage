with source as (
    select * from {{ source('tpcds', 'inventory') }}
),

renamed as (
    select * from source
)

select * from renamed
