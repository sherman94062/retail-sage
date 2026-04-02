with source as (
    select * from {{ source('tpcds', 'call_center') }}
),

renamed as (
    select * from source
)

select * from renamed
