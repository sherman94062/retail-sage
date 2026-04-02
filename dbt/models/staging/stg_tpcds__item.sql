with source as (
    select * from {{ source('tpcds', 'item') }}
),

renamed as (
    select * from source
)

select * from renamed
