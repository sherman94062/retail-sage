with source as (
    select * from {{ source('tpcds', 'warehouse') }}
),

renamed as (
    select * from source
)

select * from renamed
