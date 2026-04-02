with source as (
    select * from {{ source('tpcds', 'ship_mode') }}
),

renamed as (
    select * from source
)

select * from renamed
