with source as (
    select * from {{ source('tpcds', 'reason') }}
),

renamed as (
    select * from source
)

select * from renamed
