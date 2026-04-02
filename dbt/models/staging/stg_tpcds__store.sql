with source as (
    select * from {{ source('tpcds', 'store') }}
),

renamed as (
    select * from source
)

select * from renamed
